(ns filipesilva.sqlatom
  (:refer-clojure :exclude [atom remove list])
  (:require [clojure.edn :as edn]
            #?@(:bb [[pod.babashka.go-sqlite3 :as sqlite]]))
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicReference]
           #?@(:bb [] :clj [[java.sql DriverManager]])))

;; --- SQL abstraction ---

#?(:bb  nil
   :clj (do
          (defn- set-params! [stmt params]
            (doseq [[i p] (map-indexed vector params)]
              (.setObject stmt (inc i) p)))

          (defn- resultset->maps [rs]
            (let [md (.getMetaData rs)
                  n  (.getColumnCount md)
                  ks (mapv #(keyword (.getColumnLabel md (inc %))) (range n))]
              (loop [rows []]
                (if (.next rs)
                  (recur (conj rows (zipmap ks (mapv #(.getObject rs (inc %)) (range n)))))
                  rows))))))

(def ^:private pragmas
  ["PRAGMA journal_mode = WAL"
   "PRAGMA synchronous = NORMAL"
   "PRAGMA mmap_size = 134217728"         ; 128 megabytes
   "PRAGMA journal_size_limit = 67108864" ; 64 megabytes
   "PRAGMA cache_size = 2000"
   "PRAGMA busy_timeout = 5000"])

(def ^:private create-table-sql
  "CREATE TABLE IF NOT EXISTS atoms (key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1)")

;; bb uses a db-path instead of conn
(defn- open-db [db-path]
  #?(:bb  (do (doseq [p pragmas] (sqlite/execute! db-path [p]))
              (sqlite/execute! db-path [create-table-sql])
              db-path)
     :clj (let [conn (DriverManager/getConnection (str "jdbc:sqlite:" db-path))]
             (with-open [stmt (.createStatement conn)]
               (doseq [p pragmas] (.execute stmt p))
               (.execute stmt create-table-sql))
             conn)))

(defn- close-db [conn]
  #?(:bb  nil
     :clj (.close conn)))

(defn- sql-execute! [conn sql-params]
  #?(:bb  (:rows-affected (sqlite/execute! conn sql-params))
     :clj (with-open [stmt (.prepareStatement conn (first sql-params))]
             (set-params! stmt (rest sql-params))
             (.executeUpdate stmt))))

(defn- sql-query [conn sql-params]
  #?(:bb  (sqlite/query conn sql-params)
     :clj (with-open [stmt (.prepareStatement conn (first sql-params))]
             (set-params! stmt (rest sql-params))
             (with-open [rs (.executeQuery stmt)]
               (resultset->maps rs)))))

;; --- Helpers ---

(defn- pr-str-meta [v]
  (binding [*print-meta* true]
    (pr-str v)))

(defn- read-edn [s]
  (edn/read-string {:readers *data-readers*} s))

(def ^:private default-dir "sqlatom")

(defn- db-path
  [dir]
  (let [d (java.io.File. dir)]
    (when-not (.exists d)
      (.mkdirs d))
    (str (java.io.File. d "atoms.db"))))

(defn- db-read [conn key-str]
  (when-let [row (first (sql-query conn ["SELECT value, version FROM atoms WHERE key = ?" key-str]))]
    [(read-edn (:value row)) (:version row)]))

(defn- db-read-version [conn key-str]
  (:version (first (sql-query conn ["SELECT version FROM atoms WHERE key = ?" key-str]))))

(defn- db-cas! [conn key-str new-value new-version expected-version]
  (= 1 (sql-execute! conn ["UPDATE atoms SET value = ?, version = ? WHERE key = ? AND version = ?"
                            (pr-str-meta new-value) new-version key-str expected-version])))

(defn- db-insert-default! [conn key-str default-value]
  (sql-execute! conn ["INSERT OR IGNORE INTO atoms (key, value) VALUES (?, ?)"
                       key-str (pr-str-meta default-value)])
  nil)

(defn- throw-invalid-state! []
  (throw (IllegalStateException. "Invalid reference state")))

(defn- throw-removed! [key-str]
  (throw (IllegalStateException. (str "sqlatom key " key-str " has been removed"))))

(defn- validate [^AtomicReference validator-ref new-value]
  (when-let [vf (.get validator-ref)]
    (when-not (vf new-value)
      (throw-invalid-state!))))

(defn- cache-advance!
  "CAS-update cache only if new-ver is strictly higher. Returns true if advanced.
   When watches are provided, notifies them on advancement."
  ([^AtomicReference cache new-val new-ver]
   (loop []
     (let [current (.get cache)
           [_ cur-ver] current]
       (if (> new-ver cur-ver)
         (if (.compareAndSet cache current [new-val new-ver])
           true
           (recur))
         false))))
  ([^AtomicReference cache new-val new-ver ^ConcurrentHashMap watches atom-ref old-val]
   (when (cache-advance! cache new-val new-ver)
     (doseq [[k f] watches]
       (f k atom-ref old-val new-val))
     true)))

;; --- Implementation fns (shared between proxy/reify) ---

(defn- deref-impl [conn key-str ^AtomicReference cache ^ConcurrentHashMap watches self]
  (let [[cached-val cached-ver] (.get cache)
        db-ver (db-read-version conn key-str)]
    (when (nil? db-ver)
      (.set cache [nil -1])
      (throw-removed! key-str))
    (if (= db-ver cached-ver)
      cached-val
      (let [[new-val new-ver] (db-read conn key-str)]
        (cache-advance! cache new-val new-ver watches self cached-val)
        new-val))))

(defn- swap-impl [conn key-str ^AtomicReference cache ^AtomicReference vdtr
                  ^ConcurrentHashMap watches self apply-fn]
  (loop []
    (let [[old-val ver] (db-read conn key-str)
          _             (when (nil? ver) (throw-removed! key-str))
          new-val       (apply-fn old-val)]
      (validate vdtr new-val)
      (if (db-cas! conn key-str new-val (inc ver) ver)
        (do (cache-advance! cache new-val (inc ver) watches self old-val)
            [old-val new-val])
        (recur)))))

(defn- cas-impl [conn key-str ^AtomicReference cache ^AtomicReference vdtr
                 ^ConcurrentHashMap watches self old-val new-val]
  (loop []
    (let [[db-val ver] (db-read conn key-str)]
      (when (nil? ver) (throw-removed! key-str))
      (if (not= db-val old-val)
        (do (cache-advance! cache db-val ver)
            false)
        (do (validate vdtr new-val)
            (if (db-cas! conn key-str new-val (inc ver) ver)
              (do (cache-advance! cache new-val (inc ver) watches self old-val)
                  true)
              (recur)))))))

(defn- set-validator-impl [conn key-str ^AtomicReference vdtr vf]
  (when vf
    (let [[current-val _] (db-read conn key-str)]
      (when-not (vf current-val) (throw-invalid-state!))))
  (.set vdtr vf))

;; --- Public API ---

(defn remove
  "Removes a key from the database. Returns nil.
   Options: :dir directory."
  [key & {:keys [dir]}]
  (let [conn (open-db (db-path (or dir default-dir)))]
    (try
      (sql-execute! conn ["DELETE FROM atoms WHERE key = ?" (pr-str key)])
      nil
      (finally (close-db conn)))))

(defn list
  "Returns a sequence of all keys in the database.
   Options: :dir directory."
  [& {:keys [dir]}]
  (let [conn (open-db (db-path (or dir default-dir)))]
    (try
      (mapv #(read-edn (:key %)) (sql-query conn ["SELECT key FROM atoms"]))
      (finally (close-db conn)))))

(defn atom
  "Creates an atom backed by SQLite. key is the storage key,
   default-value is used if no value exists yet.
   The database file is <dir>/atoms.db, defaulting to sqlatom/atoms.db.
   Atom options: :meta metadata-map, :validator validate-fn
   Extra options: :dir directory"
  [key default-value & {:keys [dir meta validator]}]
  (let [conn     (open-db (db-path (or dir default-dir)))
        key-str  (pr-str key)
        _        (db-insert-default! conn key-str default-value)
        [v ver]  (db-read conn key-str)
        _        (when validator
                   (when-not (validator v) (throw-invalid-state!)))
        cache    (AtomicReference. [v ver])
        vdtr     (AtomicReference. validator)
        watches  (ConcurrentHashMap.)
        meta-ref (AtomicReference. (or meta {}))]
    #?(:bb
       ;; reify over IAtom2 only; IRef/IReference not yet supported in BB
       ;; https://github.com/babashka/babashka/issues/1931
       (reify
         clojure.lang.IAtom2
         (deref [this] (deref-impl conn key-str cache watches this))
         (swap [this f] (second (swap-impl conn key-str cache vdtr watches this f)))
         (swap [this f a] (second (swap-impl conn key-str cache vdtr watches this #(f % a))))
         (swap [this f a b] (second (swap-impl conn key-str cache vdtr watches this #(f % a b))))
         (swap [this f x y & args] (second (swap-impl conn key-str cache vdtr watches this #(apply f % x y args))))
         (compareAndSet [this o n] (cas-impl conn key-str cache vdtr watches this o n))
         (reset [this v] (second (swap-impl conn key-str cache vdtr watches this (constantly v))))
         (swapVals [this f] (swap-impl conn key-str cache vdtr watches this f))
         (swapVals [this f a] (swap-impl conn key-str cache vdtr watches this #(f % a)))
         (swapVals [this f a b] (swap-impl conn key-str cache vdtr watches this #(f % a b)))
         (swapVals [this f x y & args] (swap-impl conn key-str cache vdtr watches this #(apply f % x y args)))
         (resetVals [this v] (swap-impl conn key-str cache vdtr watches this (constantly v))))

       :clj
       (proxy [clojure.lang.IAtom2 clojure.lang.IRef clojure.lang.IReference] []
         (deref [] (deref-impl conn key-str cache watches this))
         (swap
           ([f]          (second (swap-impl conn key-str cache vdtr watches this f)))
           ([f a]        (second (swap-impl conn key-str cache vdtr watches this #(f % a))))
           ([f a b]      (second (swap-impl conn key-str cache vdtr watches this #(f % a b))))
           ([f x y args] (second (swap-impl conn key-str cache vdtr watches this #(apply f % x y args)))))
         (compareAndSet [o n] (cas-impl conn key-str cache vdtr watches this o n))
         (reset [v] (second (swap-impl conn key-str cache vdtr watches this (constantly v))))
         (swapVals
           ([f]          (swap-impl conn key-str cache vdtr watches this f))
           ([f a]        (swap-impl conn key-str cache vdtr watches this #(f % a)))
           ([f a b]      (swap-impl conn key-str cache vdtr watches this #(f % a b)))
           ([f x y args] (swap-impl conn key-str cache vdtr watches this #(apply f % x y args))))
         (resetVals [v] (swap-impl conn key-str cache vdtr watches this (constantly v)))

         (setValidator [vf] (set-validator-impl conn key-str vdtr vf))
         (getValidator [] (.get vdtr))
         (getWatches [] (into {} watches))
         (addWatch [k f] (.put watches k f) this)
         (removeWatch [k] (.remove watches k) this)

         (meta [] (.get meta-ref))
         (alterMeta [f args]
           (locking meta-ref
             (let [new-meta (apply f (.get meta-ref) args)]
               (.set meta-ref new-meta)
               new-meta)))
         (resetMeta [m]
           (.set meta-ref m)
           m)))))
