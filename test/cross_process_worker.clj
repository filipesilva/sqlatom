(require '[filipesilva.sqlatom :as sqlatom])

(def id (random-uuid))

(let [[dir n-str] *command-line-args*
      n (parse-long n-str)
      a (sqlatom/atom :counter 0 :dir dir)]
  (dotimes [i n]
    (println id i)
    (swap! a inc)))
