(ns roundtrip
  (:require [criterium.core :as cc]
            [deed.core :as deed]
            [fast-edn.core :as edn]
            [filipesilva.fast-pr-str :as fast-pr-str]
            [fipp.edn :as fipp]
            [me.flowthing.pp :as pp]
            [taoensso.nippy :as nippy]))

(defn -main [& _]
  (let [data (edn/read-string
              ;; Parse edn, stripping unknown tags (e.g. #datascript/DB) to plain values
              {:default (fn [_ v] v)}
              (slurp "bench/roam-book-club-2026-02-18-11-31-58.edn"))]
    (println "\n=== nippy/thaw ∘ nippy/freeze ===")
    (cc/quick-bench (nippy/thaw (nippy/freeze data)))

    (println "\n=== deed/decode-from ∘ deed/encode-to-bytes ===")
    (cc/quick-bench (deed/decode-from (deed/encode-to-bytes data)))

    (println "\n=== fast-edn/read-string ∘ pr-str ===")
    (cc/quick-bench (edn/read-string (pr-str data)))

    (println "\n=== fast-edn/read-string ∘ fast-pr-str/pr-str ===")
    (cc/quick-bench (edn/read-string (fast-pr-str/pr-str data)))

    (println "\n=== fast-edn/read-string ∘ fipp/pprint ===")
    (cc/quick-bench (edn/read-string (with-out-str (fipp/pprint data))))

    (println "\n=== fast-edn/read-string ∘ pp/pprint ===")
    (cc/quick-bench (edn/read-string (with-out-str (pp/pprint data)))))
  (shutdown-agents))
