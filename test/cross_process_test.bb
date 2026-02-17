#!/usr/bin/env bb

(require '[babashka.process :as p]
         '[babashka.fs :as fs])

(def test-dir (str (fs/create-temp-dir {:prefix "sqlatom-test-"})))
(def n-procs 30)
(def n-incs 50)
(def expected (* n-procs n-incs))

(println (str "Testing " n-procs " processes x " n-incs " increments = " expected " expected"))

(try
  ;; launch workers in parallel
  (let [procs (mapv (fn [_]
                      (p/process ["clojure" "-M" "test/cross_process_worker.clj" test-dir (str n-incs)]))
                    (range n-procs))]
    (doseq [proc procs]
      (let [{:keys [exit]} @proc]
        (when (not= 0 exit)
          (println "Worker failed!")
          (System/exit 1)))))

  ;; read final value
  (let [{:keys [out]} (p/shell {:out :string}
                               "clojure" "-M" "-e"
                               (str "(require '[filipesilva.sqlatom :as sa])"
                                    "(print @(sa/atom :counter 0 :dir \"" test-dir "\"))"))
        value (parse-long (str/trim out))]
    (if (= value expected)
      (println (str "PASS: counter = " value))
      (do (println (str "FAIL: counter = " value " (expected " expected ")"))
          (System/exit 1))))

  (finally
    (fs/delete-tree test-dir)))
