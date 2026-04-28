(ns bench
  (:require [filipesilva.sqlatom :as sqlatom]
            [clojure.edn :as edn]
            [criterium.core :as criterium]))

(defn run
  ([] (run "bench/roam-book-club-2026-02-18-11-31-58.edn"))
  ([path]
   (println "Reading" path "...")
   (let [raw  (slurp path)
         _    (printf "  File size: %.1f MB%n" (/ (count raw) 1e6))
         ;; Parse edn, stripping unknown tags (e.g. #datascript/DB) to plain values
         data (edn/read-string {:default (fn [_ v] v)} raw)
         _    (printf "  Top-level keys: %s%n" (if (map? data) (pr-str (keys data)) "(not a map)"))
         k    ::large
         _    (sqlatom/remove k)
         a    (sqlatom/atom k {})]
     (try
       (println "\nreset!:")
       (criterium/bench (reset! a data))

       (println "\nswap!:")
       (criterium/bench (swap! a assoc :perf-test-key "hello"))

       (finally
         (sqlatom/remove k))))))

(defn -main [& args]
  (apply run args)
  (shutdown-agents))
