(ns performance-test
  (:require [filipesilva.sqlatom :as sqlatom]
            [clojure.edn :as edn]))

(defn- time-ms [f]
  (let [start (System/nanoTime)
        ret   (f)
        end   (System/nanoTime)]
    {:result ret :ms (/ (- end start) 1e6)}))

(let [path "test/roam-book-club-2026-02-18-11-31-58.edn"
      _    (println "Reading" path "...")
      raw  (slurp path)
      _    (printf "  File size: %.1f MB%n" (/ (count raw) 1e6))

      ;; Parse edn, stripping unknown tags (e.g. #datascript/DB) to plain values
      {:keys [ms result]} (time-ms #(edn/read-string {:default (fn [_ v] v)} raw))
      data result
      _    (printf "  EDN parse: %.0f ms%n" ms)
      _    (printf "  Top-level keys: %s%n" (if (map? data) (pr-str (keys data)) "(not a map)"))

      ;; Setup sqlatom
      dir  "sqlatom-perf-test"
      k    ::large
      _    (sqlatom/remove k :dir dir)
      a    (sqlatom/atom k {} :dir dir)]

  (try
    ;; Time reset!
    (let [{:keys [ms]} (time-ms #(reset! a data))]
      (printf "%nreset!: %.0f ms%n" ms))

    ;; Time swap! adding a toplevel key
    (let [{:keys [ms]} (time-ms #(swap! a assoc :perf-test-key "hello"))]
      (printf "swap!:  %.0f ms%n" ms))

    (finally
      (sqlatom/remove k :dir dir)
      (doseq [f (.listFiles (java.io.File. dir))]
        (.delete f))
      (.delete (java.io.File. dir)))))
