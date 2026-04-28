(ns probe
  "Time each step of a swap! to see where time is spent.
   Run with: bb probe"
  (:require [filipesilva.sqlatom :as sqlatom]
            [fast-edn.core :as fast-edn]
            [clojure.edn :as edn]))

(def ^:dynamic *total* nil)

(defmacro timed [label expr]
  `(let [start# (System/nanoTime)
         ret#   ~expr
         ms#    (/ (- (System/nanoTime) start#) 1e6)]
     (when *total* (swap! *total* + ms#))
     (printf "  %-18s %7.1f ms%n" ~label ms#)
     ret#))

(defn run
  ([] (run "bench/roam-book-club-2026-02-18-11-31-58.edn"))
  ([path]
   (println "Reading" path "...")
   (let [raw  (slurp path)
         _    (printf "  File size: %.1f MB%n" (/ (count raw) 1e6))
         data (fast-edn/read-string {:default (fn [_ v] v)} raw)
         k    ::probe
         _    (sqlatom/remove k)
         a    (sqlatom/atom k {})]
     (try
       (reset! a data)

       (println "\nSingle swap! (warm):")
       (timed "swap!" (swap! a assoc :probe-key "x"))

       (let [c     (java.sql.DriverManager/getConnection "jdbc:sqlite:sqlatom/atoms.db")
             k-str (pr-str k)]
         (try
           (let [raw-row (with-open [stmt (.prepareStatement c "SELECT value FROM atoms WHERE key = ?")]
                           (.setString stmt 1 k-str)
                           (with-open [rs (.executeQuery stmt)]
                             (.next rs)
                             (.getString rs 1)))]
             (printf "%nBreakdown of swap! steps (row size %.1f MB):%n"
                     (/ (count raw-row) 1e6))
             (binding [*total* (atom 0.0)]
               (timed "SELECT raw"
                 (with-open [stmt (.prepareStatement c "SELECT value FROM atoms WHERE key = ?")]
                   (.setString stmt 1 k-str)
                   (with-open [rs (.executeQuery stmt)]
                     (.next rs)
                     (.getString rs 1))))
               (let [parsed  (timed "fast-edn read"
                               (fast-edn/read-string {:default (fn [_ v] v)} raw-row))
                     new-val (timed "apply-fn"
                               (assoc parsed :probe-key "x"))
                     written (timed "pr-str-meta"
                               (binding [*print-meta* true] (pr-str new-val)))]
                 (timed "UPDATE"
                   (with-open [stmt (.prepareStatement c "UPDATE atoms SET value = ?, version = version + 1 WHERE key = ?")]
                     (.setString stmt 1 written)
                     (.setString stmt 2 k-str)
                     (.executeUpdate stmt))))
               (printf "  %-18s %7.1f ms%n" "TOTAL" @*total*))

             (println "\nReader comparison on the same row:")
             (timed "fast-edn read"
               (fast-edn/read-string {:default (fn [_ v] v)} raw-row))
             (timed "clojure.edn read"
               (edn/read-string {:default (fn [_ v] v)} raw-row)))
           (finally (.close c))))
       (finally
         (sqlatom/remove k))))))

(defn -main [& args]
  (apply run args)
  (shutdown-agents))
