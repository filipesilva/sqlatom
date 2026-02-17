(require '[filipesilva.sqlatom :as sqlatom])

(let [[dir n-str] *command-line-args*
      n (parse-long n-str)
      a (sqlatom/atom :counter 0 :dir dir)]
  (dotimes [_ n]
    (swap! a inc)))
