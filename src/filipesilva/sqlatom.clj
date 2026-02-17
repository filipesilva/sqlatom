(ns filipesilva.sqlatom
  (:refer-clojure :exclude [atom remove list])
  (:require [clojure.edn :as edn])
  (:import [java.sql DriverManager]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicReference]))

;; --- Connection management ---

(defonce ^:private connections (clojure.core/atom {}))

(defn- get-connection [db-path]
  (or (get @connections db-path)
      (locking connections
        (or (get @connections db-path)
            (let [conn (DriverManager/getConnection (str "jdbc:sqlite:" db-path))]
              (with-open [stmt (.createStatement conn)]
                ;; same as rails 8.0
                (.execute stmt "PRAGMA foreign_keys = ON")
                (.execute stmt " PRAGMA journal_mode = WAL")
                (.execute stmt " PRAGMA synchronous = NORMAL")
                (.execute stmt " PRAGMA mmap_size = 134217728") ; 128 megabytes
                (.execute stmt " PRAGMA journal_size_limit = 67108864") ; 64 megabytes
                (.execute stmt " PRAGMA cache_size = 2000")
                ;; create atoms table
                (.execute stmt "CREATE TABLE IF NOT EXISTS atoms (key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1)"))
              (swap! connections assoc db-path conn)
              conn)))))

(defn- pr-str-meta [v]
  (binding [*print-meta* true]
    (pr-str v)))

;; --- DB helpers ---

(defn- db-read [conn key-str]
  (with-open [stmt (.prepareStatement conn "SELECT value, version FROM atoms WHERE key = ?")]
    (.setString stmt 1 key-str)
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        [(edn/read-string {:readers *data-readers*} (.getString rs "value"))
         (.getLong rs "version")]))))

(defn- db-read-version [conn key-str]
  (with-open [stmt (.prepareStatement conn "SELECT version FROM atoms WHERE key = ?")]
    (.setString stmt 1 key-str)
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getLong rs "version")))))

(defn- db-cas! [conn key-str new-value new-version expected-version]
  (with-open [stmt (.prepareStatement conn "UPDATE atoms SET value = ?, version = ? WHERE key = ? AND version = ?")]
    (.setString stmt 1 (pr-str-meta new-value))
    (.setLong stmt 2 new-version)
    (.setString stmt 3 key-str)
    (.setLong stmt 4 expected-version)
    (= 1 (.executeUpdate stmt))))

(defn- db-insert-default! [conn key-str default-value]
  (with-open [stmt (.prepareStatement conn "INSERT OR IGNORE INTO atoms (key, value) VALUES (?, ?)")]
    (.setString stmt 1 key-str)
    (.setString stmt 2 (pr-str-meta default-value))
    (.executeUpdate stmt)
    nil))

;; --- Helpers ---

(defn- throw-invalid-state! []
  (throw (IllegalStateException. "Invalid reference state")))

(defn- throw-removed! [key-str]
  (throw (IllegalStateException. (str "sqlatom key " key-str " has been removed"))))

(defn- validate [^AtomicReference validator-ref new-value]
  (when-let [vf (.get validator-ref)]
    (when-not (vf new-value)
      (throw-invalid-state!))))

(defn- notify-watches [^ConcurrentHashMap watches atom-ref old-value new-value]
  (doseq [[k f] watches]
    (f k atom-ref old-value new-value)))

(defn- do-swap
  "CAS retry loop. apply-fn takes old-value, returns new-value.
   Returns [old-value new-value]."
  [conn key-str ^AtomicReference cache ^AtomicReference validator-ref
   ^ConcurrentHashMap watches atom-ref apply-fn]
  (loop []
    (let [[old-val ver] (db-read conn key-str)
          _ (when (nil? ver) (throw-removed! key-str))
          new-val (apply-fn old-val)]
      (validate validator-ref new-val)
      (if (db-cas! conn key-str new-val (inc ver) ver)
        (do (.set cache [new-val (inc ver)])
            (notify-watches watches atom-ref old-val new-val)
            [old-val new-val])
        (recur)))))

;; --- Public API ---

(def ^:private default-dir "sqlatom")

(defn- db-path
  ([] (db-path default-dir))
  ([dir]
   (let [d (java.io.File. dir)]
     (when-not (.exists d)
       (.mkdirs d))
     (str (java.io.File. d "atoms.db")))))

(defn remove
  "Removes a key from the database. Returns nil.
   Options: :dir directory."
  [key & {:keys [dir]}]
  (let [conn (get-connection (db-path (or dir default-dir)))]
    (with-open [stmt (.prepareStatement conn "DELETE FROM atoms WHERE key = ?")]
      (.setString stmt 1 (pr-str key))
      (.executeUpdate stmt)
      nil)))

(defn list
  "Returns a sequence of all keys in the database.
   Options: :dir directory."
  [& {:keys [dir]}]
  (let [conn (get-connection (db-path (or dir default-dir)))]
    (with-open [stmt (.prepareStatement conn "SELECT key FROM atoms")]
      (with-open [rs (.executeQuery stmt)]
        (loop [ks []]
          (if (.next rs)
            (recur (conj ks (edn/read-string {:readers *data-readers*} (.getString rs "key"))))
            ks))))))

(defn atom
  "Creates an atom backed by SQLite. key is the storage key,
   default-value is used if no value exists yet.
   The database file is <dir>/atoms.db, defaulting to sqlatom/atoms.db.
   Atom options: :meta metadata-map, :validator validate-fn
   Extra options: :dir directory"
  [key default-value & {:keys [dir meta validator]}]
  (let [db-path  (db-path (or dir default-dir))
        conn     (get-connection db-path)
        key-str  (pr-str key)
        _        (db-insert-default! conn key-str default-value)
        [v ver]  (db-read conn key-str)
        _        (when validator
                   (when-not (validator v) (throw-invalid-state!)))
        cache    (AtomicReference. [v ver])
        vdtr     (AtomicReference. validator)
        watches  (ConcurrentHashMap.)
        meta-ref (AtomicReference. (or meta {}))]
    (proxy [Object clojure.lang.IAtom2 clojure.lang.IRef clojure.lang.IReference] []

      ;; IDeref
      (deref []
        (let [[cached-val cached-ver] (.get cache)
              db-ver (db-read-version conn key-str)]
          (when (nil? db-ver)
            (.set cache [nil -1])
            (throw-removed! key-str))
          (if (= db-ver cached-ver)
            cached-val
            (let [[new-val new-ver] (db-read conn key-str)]
              (.set cache [new-val new-ver])
              (notify-watches watches this cached-val new-val)
              new-val))))

      ;; IAtom
      (swap
        ([f]
         (second (do-swap conn key-str cache vdtr watches this #(f %))))
        ([f arg]
         (second (do-swap conn key-str cache vdtr watches this #(f % arg))))
        ([f arg1 arg2]
         (second (do-swap conn key-str cache vdtr watches this #(f % arg1 arg2))))
        ([f x y args]
         (second (do-swap conn key-str cache vdtr watches this #(apply f % x y args)))))

      (compareAndSet [old-val new-val]
        (let [[db-val ver] (db-read conn key-str)]
          (when (nil? ver) (throw-removed! key-str))
          (if (not= db-val old-val)
            (do (.set cache [db-val ver])
                false)
            (do (validate vdtr new-val)
                (let [ok (db-cas! conn key-str new-val (inc ver) ver)]
                  (when ok
                    (.set cache [new-val (inc ver)])
                    (notify-watches watches this old-val new-val))
                  ok)))))

      (reset [new-val]
        (second (do-swap conn key-str cache vdtr watches this (constantly new-val))))

      ;; IAtom2
      (swapVals
        ([f]
         (do-swap conn key-str cache vdtr watches this #(f %)))
        ([f arg]
         (do-swap conn key-str cache vdtr watches this #(f % arg)))
        ([f arg1 arg2]
         (do-swap conn key-str cache vdtr watches this #(f % arg1 arg2)))
        ([f x y args]
         (do-swap conn key-str cache vdtr watches this #(apply f % x y args))))

      (resetVals [new-val]
        (do-swap conn key-str cache vdtr watches this (constantly new-val)))

      ;; IRef
      (setValidator [vf]
        (when vf
          (let [[current-val _] (db-read conn key-str)]
            (when-not (vf current-val) (throw-invalid-state!))))
        (.set vdtr vf))

      (getValidator [] (.get vdtr))

      (getWatches [] (into {} watches))

      (addWatch [k f]
        (.put watches k f)
        this)

      (removeWatch [k]
        (.remove watches k)
        this)

      ;; IMeta
      (meta [] (.get meta-ref))

      ;; IReference
      (alterMeta [f args]
        (locking meta-ref
          (let [new-meta (apply f (.get meta-ref) args)]
            (.set meta-ref new-meta)
            new-meta)))

      (resetMeta [m]
        (.set meta-ref m)
        m))))
