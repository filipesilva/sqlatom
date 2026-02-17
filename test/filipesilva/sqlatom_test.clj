(ns filipesilva.sqlatom-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [filipesilva.sqlatom :as sqlatom]))

(def ^:private test-ns (namespace ::_))

(use-fixtures :each
  (fn [f]
    (doseq [k (sqlatom/list)
            :when (= (namespace k) test-ns)]
      (sqlatom/remove k))
    (f)))

(deftest create-and-deref-test

  (let [a (sqlatom/atom ::x {:count 0})]
    (is (= {:count 0} @a))))

(deftest swap-test
  (let [a (sqlatom/atom ::x {:count 0})]
    (testing "swap! with fn"
      (is (= {:count 1} (swap! a update :count inc)))
      (is (= {:count 1} @a)))
    (testing "swap! with fn and arg"
      (is (= {:count 1 :a 1} (swap! a assoc :a 1)))
      (is (= {:count 1 :a 1} @a)))
    (testing "swap! with fn and two args"
      (swap! a assoc :b 2 :c 3)
      (is (= {:count 1 :a 1 :b 2 :c 3} @a)))
    (testing "swap! with fn and varargs"
      (swap! a merge {:d 4} {:e 5})
      (is (= {:count 1 :a 1 :b 2 :c 3 :d 4 :e 5} @a)))))

(deftest reset-test
  (let [a (sqlatom/atom ::x {:count 0})]
    (is (= {:new true} (reset! a {:new true})))
    (is (= {:new true} @a))))

(deftest compare-and-set-test
  (let [a (sqlatom/atom ::x {:count 0})]
    (testing "returns false when old value doesn't match"
      (is (false? (compare-and-set! a {:wrong true} {:count 1})))
      (is (= {:count 0} @a)))
    (testing "returns true when old value matches"
      (is (true? (compare-and-set! a {:count 0} {:count 1})))
      (is (= {:count 1} @a)))))

(deftest swap-vals-test
  (let [a (sqlatom/atom ::x {:count 0})]
    (is (= [{:count 0} {:count 1}] (swap-vals! a update :count inc)))
    (is (= [{:count 1} {:count 2}] (swap-vals! a update :count inc)))))

(deftest reset-vals-test
  (let [a (sqlatom/atom ::x {:count 0})]
    (is (= [{:count 0} {:done true}] (reset-vals! a {:done true})))
    (is (= {:done true} @a))))

(deftest watch-test
  (let [a        (sqlatom/atom ::x 0)
        log      (clojure.core/atom [])
        watch-fn (fn [k _r old new]
                   (swap! log conj [k old new]))]
    (add-watch a :w watch-fn)
    (swap! a inc)
    (swap! a inc)
    (is (= [[:w 0 1] [:w 1 2]] @log))
    (remove-watch a :w)
    (swap! a inc)
    (is (= [[:w 0 1] [:w 1 2]] @log) "no more watch calls after remove")))

(deftest validator-test
  (let [a (sqlatom/atom ::x 1)]
    (set-validator! a pos?)
    (testing "valid value succeeds"
      (is (= 5 (reset! a 5))))
    (testing "invalid value throws"
      (is (thrown? IllegalStateException (reset! a -1)))
      (is (= 5 @a) "value unchanged after failed validation"))
    (testing "setting validator that rejects current value throws"
      (is (thrown? IllegalStateException (set-validator! a neg?))))))

(deftest meta-test
  (let [a (sqlatom/atom ::x 0)]
    (is (= {} (meta a)))
    (reset-meta! a {:my :meta})
    (is (= {:my :meta} (meta a)))
    (alter-meta! a assoc :extra true)
    (is (= {:my :meta :extra true} (meta a)))))

(deftest meta-option-test
  (let [a (sqlatom/atom ::x 0 :meta {:created true})]
    (is (= {:created true} (meta a)))))

(deftest validator-option-test
  (testing "validator option on creation"
    (let [a (sqlatom/atom ::x 1 :validator pos?)]
      (is (= 1 @a))
      (is (thrown? IllegalStateException (reset! a -1)))))
  (testing "validator rejects initial value"
    (sqlatom/remove ::x)
    (is (thrown? IllegalStateException
                (sqlatom/atom ::x -1 :validator pos?)))))

(deftest persistence-test
  (testing "second atom with same key reads persisted value"
    (let [a (sqlatom/atom ::x 0)]
      (reset! a 42))
    (let [b (sqlatom/atom ::x 0)]
      (is (= 42 @b) "default ignored, persisted value returned"))))

(deftest multiple-keys-test
  (let [a (sqlatom/atom ::a 1)
        b (sqlatom/atom ::b 2)]
    (swap! a + 10)
    (swap! b + 20)
    (is (= 11 @a))
    (is (= 22 @b))))

(deftest remove-test
  (sqlatom/atom ::x 42)
  (is (= [::x] (sqlatom/list)))
  (sqlatom/remove ::x)
  (is (= [] (sqlatom/list)))
  (testing "re-creating after remove uses new default"
    (let [a (sqlatom/atom ::x 0)]
      (is (= 0 @a)))))

(deftest list-test
  (is (= [] (sqlatom/list)))
  (sqlatom/atom ::b 2)
  (sqlatom/atom ::a 1)
  (is (= #{::a ::b} (set (sqlatom/list)))))

(deftest value-metadata-test
  (testing "metadata on default value persists"
    (let [a (sqlatom/atom ::x (with-meta {:count 0} {:origin "test"}))]
      (is (= {:origin "test"} (clojure.core/meta @a)))))
  (testing "metadata on swapped value persists"
    (let [a (sqlatom/atom ::x 0)]
      (reset! a (with-meta {:count 1} {:tag :swapped}))
      (is (= {:tag :swapped} (clojure.core/meta @a)))))
  (testing "metadata survives re-open"
    (let [a (sqlatom/atom ::y (with-meta [1 2] {:src :init}))]
      (reset! a (with-meta [3 4] {:src :updated})))
    (let [b (sqlatom/atom ::y nil)]
      (is (= [3 4] @b))
      (is (= {:src :updated} (clojure.core/meta @b))))))

(deftest dir-option-test
  (let [dir "sqlatom-test-custom"
        a   (sqlatom/atom ::x 42 :dir dir)]
    (try
      (is (= 42 @a))
      (is (.exists (java.io.File. dir "atoms.db")))
      (finally
        (doseq [f (.listFiles (java.io.File. dir))]
          (.delete f))
        (.delete (java.io.File. dir))))))
