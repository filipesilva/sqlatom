(ns filipesilva.fast-pr-str
  "Fast EDN-printing variant of clojure.core/pr-str on the JVM. Based on
   conversation at
   https://clojurians.slack.com/archives/C03S1KBA2/p1777375536274579

   The trick (per clojure-slack feedback): one shared StringBuilder,
   append directly, never call str on nested values. Avoids the
   per-nested-object StringBuilder allocations that pr does internally.

   Handles in the fast path: nil, booleans, numbers (Long/Double/BigInt/
   BigDecimal/Ratio etc., including ##NaN/##Inf/##-Inf), strings (with
   escapes), keywords, symbols, maps, vectors, sets, lists/seqs,
   characters, java.util.UUID, java.util.Date, and metadata. Records
   and everything else (regex, vars, exceptions, custom types) fall
   back to clojure.core/pr-str so any registered `print-method` is
   honored.

   Honors `*print-meta*`.

   NOT a full pr-str replacement — these dynamic-var knobs are ignored:
   - `*print-length*` — output is never truncated with `...`.
   - `*print-level*`  — output is never truncated with `#`.
   - `*print-dup*`    — no `#=(...)` evaluator forms emitted; type
                        fidelity for non-EDN-native types should be
                        achieved via tagged literals + data readers
                        instead (which we do support via the fallback).
   - `*print-namespace-maps*` — maps are always emitted in long form
                                (`{:ns/k v}`), never as `#:ns{:k v}`.

   Note: this namespace loads and runs correctly under Babashka, but on
   BB it is roughly 20× slower than the built-in `clojure.core/pr-str`
   because each cond branch is interpreted by sci. Use stock pr-str on
   BB; only use this on the JVM."
  (:refer-clojure :exclude [pr-str]))

(declare write!)

(def ^:private ^java.lang.ThreadLocal inst-format
  (proxy [java.lang.ThreadLocal] []
    (initialValue []
      (doto (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS-00:00")
        (.setTimeZone (java.util.TimeZone/getTimeZone "UTC"))))))

(def ^:private char-name
  ;; Match Clojure's char-name-string for reader compatibility.
  {\newline   "newline"
   \tab       "tab"
   \space     "space"
   \backspace "backspace"
   \formfeed  "formfeed"
   \return    "return"})

(defn- write-str!
  "Bulk-copy unescaped runs via StringBuilder.append(CharSequence, int, int);
   only stop to emit an escape for the seven chars Clojure escapes."
  [^StringBuilder sb ^String s]
  (.append sb \")
  (let [n (.length s)]
    (loop [start 0, i 0]
      (if (>= i n)
        (when (< start n)
          (.append sb ^CharSequence s (int start) (int n)))
        (let [c   (.charAt s i)
              esc (case c
                    \\         "\\\\"
                    \"         "\\\""
                    \newline   "\\n"
                    \return    "\\r"
                    \tab       "\\t"
                    \formfeed  "\\f"
                    \backspace "\\b"
                    nil)]
          (if esc
            (do
              (when (< start i)
                (.append sb ^CharSequence s (int start) (int i)))
              (.append sb ^String esc)
              (recur (inc i) (inc i)))
            (recur start (inc i)))))))
  (.append sb \"))

(defn- write-named! [^StringBuilder sb v]
  (when-let [ns (namespace v)]
    (.append sb ^String ns)
    (.append sb \/))
  (.append sb ^String (name v)))

(defn- write-keyword! [^StringBuilder sb kw]
  (.append sb \:)
  (write-named! sb kw))

(defn- write-coll! [^StringBuilder sb ^String open ^String close coll]
  (.append sb open)
  (reduce
    (fn [first? x]
      (when-not first? (.append sb \space))
      (write! sb x)
      false)
    true coll)
  (.append sb close))

(defn- write-map! [^StringBuilder sb m]
  (.append sb \{)
  (reduce-kv
    (fn [first? k v]
      (when-not first? (.append sb ", "))
      (write! sb k)
      (.append sb \space)
      (write! sb v)
      false)
    true m)
  (.append sb \}))

(defn- write-meta! [^StringBuilder sb v]
  (when *print-meta*
    (when-let [m (and (instance? clojure.lang.IObj v)
                      (.meta ^clojure.lang.IObj v))]
      (when (pos? (count m))
        (.append sb \^)
        (write! sb m)
        (.append sb \space)))))

(defn- write! [^StringBuilder sb v]
  ;; Branches ordered by approximate frequency in datoms-heavy data:
  ;; Long, Keyword, String, vectors first; rare types at the bottom.
  (cond
    (instance? Long v)                 (.append sb (long v))
    (keyword? v)                       (write-keyword! sb v)
    (string? v)                        (write-str! sb v)
    (vector? v)                        (do (write-meta! sb v)
                                           (write-coll! sb "[" "]" v))
    ;; Records implement IPersistentMap but should be printed via their
    ;; print-method (often as a tagged literal). Fall through to clojure.core/pr-str.
    (record? v)                        (.append sb (clojure.core/pr-str v))
    (map? v)                           (do (write-meta! sb v)
                                           (write-map! sb v))
    (nil? v)                           (.append sb "nil")
    (true? v)                          (.append sb "true")
    (false? v)                         (.append sb "false")
    (instance? Double v)               (let [d (double v)]
                                         (cond
                                           (Double/isNaN d)               (.append sb "##NaN")
                                           (= d Double/POSITIVE_INFINITY) (.append sb "##Inf")
                                           (= d Double/NEGATIVE_INFINITY) (.append sb "##-Inf")
                                           :else                          (.append sb d)))
    (set? v)                           (do (write-meta! sb v)
                                           (write-coll! sb "#{" "}" v))
    (symbol? v)                        (do (write-meta! sb v)
                                           (write-named! sb v))
    (seq? v)                           (do (write-meta! sb v)
                                           (write-coll! sb "(" ")" v))
    (list? v)                          (do (write-meta! sb v)
                                           (write-coll! sb "(" ")" v))
    (instance? java.util.UUID v)       (do (.append sb "#uuid \"")
                                           (.append sb (.toString ^java.util.UUID v))
                                           (.append sb \"))
    (instance? java.util.Date v)       (do (.append sb "#inst \"")
                                           (.append sb ^String (.format ^java.text.SimpleDateFormat
                                                                        (.get inst-format) v))
                                           (.append sb \"))
    (char? v)                          (do (.append sb \\)
                                           (if-let [n (char-name v)]
                                             (.append sb ^String n)
                                             (.append sb (char v))))
    (instance? clojure.lang.BigInt v)  (do (.append sb (str v)) (.append sb \N))
    (instance? java.math.BigDecimal v) (do (.append sb (str v)) (.append sb \M))
    (number? v)                        (.append sb (str v))
    ;; Fallback: anything with a registered print-method (regex, custom
    ;; types, etc.). Inherits the ambient *print-meta*.
    :else                              (.append sb (clojure.core/pr-str v))))

(defn pr-str
  "Like clojure.core/pr-str but faster on big nested EDN values.
   See the namespace docstring for what is and isn't supported."
  ^String [& xs]
  (let [sb (StringBuilder.)]
    (loop [xs (seq xs), first? true]
      (when xs
        (when-not first? (.append sb \space))
        (write! sb (first xs))
        (recur (next xs) false)))
    (.toString sb)))
