(ns jepsen.swytch.workload.elle-ssi
  "Interactive Redis SSI transactions checked for serializability by Elle.

  No WATCH, Lua, or client-side retries: conflict detection must come from
  SSI itself. Reads are observed before EXEC on the MULTI snapshot, and are
  recorded as committed only after EXEC succeeds. Final observations use
  independent, non-transactional reads of each key. A passing finite history
  is evidence of serializability, not a proof for all possible executions."
  (:require [elle.list-append :as elle]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [store :as store]]
            [jepsen.swytch.db :as sdb]
            [taoensso.carmine :as car])
  (:import [java.io IOException]))

(def key-count 5)
(def consistency-models [:serializable])

(defn redis-key [k] (str "ssi-" k))

(defn require-reply!
  "Reject protocol mismatches (especially QUEUED) instead of silently testing
  ordinary Redis transactions. Redis error replies retain uncertain status."
  [pred reply]
  (when (instance? Exception reply)
    (throw reply))
  (when-not (pred reply)
    (throw (ex-info "Unexpected SSI reply"
                    {:type :ssi-protocol-error :reply reply})))
  reply)

(defn run-txn!
  "Runs commands synchronously on one connection. Returns completed micro-ops,
  or nil for a definite conflict abort. command! is injectable for wire tests."
  [command! txn]
  (require-reply! #{"OK"} (command! ["MULTI"]))
  (let [replies (mapv (fn [[f k v]]
                        (case f
                          :r (require-reply! vector?
                               (command! ["LRANGE" (redis-key k) 0 -1]))
                          :append (require-reply! #(and (integer? %) (pos? %))
                                    (command! ["RPUSH" (redis-key k) (str v)]))))
                      txn)
        committed (command! ["EXEC"])]
    (when (some? committed)
      ;; EXEC must confirm exactly the results observed interactively; it
      ;; must not re-execute reads against a newer snapshot.
      (require-reply! #(and (vector? %) (= replies %)) committed)
      (mapv (fn [[f k v] reply]
              (case f
                :r [:r k (mapv #(require-reply! integer? (parse-long (str %))) reply)]
                :append [:append k v]))
            txn replies))))

(defn exec-txn [conn txn]
  ;; A fresh, unpooled connection prevents an unfinished transaction from
  ;; leaking into the next operation after an error. with-replies flushes
  ;; each command immediately while wcar keeps the same connection bound.
  (car/wcar (assoc conn :pool :none)
    (car/return
      (run-txn! (fn [command]
                  (car/with-replies (car/redis-call command)))
                txn))))

(defn run-final-read!
  "One ordinary LRANGE is one Elle operation; never imply multi-key atomicity."
  [command! txn]
  (require-reply! #(and (= 1 (count %)) (= :r (ffirst %))) txn)
  (let [[[_ k _]] txn
        reply (require-reply! vector? (command! ["LRANGE" (redis-key k) 0 -1]))]
    [[:r k (mapv #(require-reply! integer? (parse-long (str %))) reply)]]))

(defn exec-final-read [conn txn]
  (car/wcar (assoc conn :pool :none)
    (car/return
      (run-final-read! (fn [command]
                         (car/with-replies (car/redis-call command)))
                       txn))))

(defrecord SSIClient [conn node]
  client/Client
  (open! [this test node]
    (assoc this :node node
                :conn {:spec {:host node
                              :port (:port-override test sdb/redis-port)
                              :conn-timeout-ms 5000
                              :read-timeout-ms 5000}}))
  (setup! [_ _])
  (invoke! [_ _ op]
    (let [op (assoc op :node node)]
      (try
        (if-let [value ((if (:final-read op) exec-final-read exec-txn)
                       conn (:value op))]
          (assoc op :type :ok :value value)
          (assoc op :type :fail :error :ssi-conflict))
        ;; A lost EXEC reply may have committed. Keep the original append
        ;; inputs so Elle can infer its outcome from later reads.
        (catch clojure.lang.ExceptionInfo e
          (assoc op :type :info
                    :error (or (:type (ex-data e)) (.getMessage e))))
        (catch IOException e
          (assoc op :type :info :error (.getMessage e))))))
  (teardown! [_ _])
  (close! [_ _]))

(defn txn-generator
  "Mix random multi-key transactions with opposing read/append pairs. Two
  concurrent pairs r(x),append(y) and r(y),append(x) expose write skew:
  SI permits their rw cycle, while serializability forbids it."
  []
  (let [next-value (atom 0)]
    (fn [_ _]
      {:type :invoke :f :txn
       :value
       (if (< (rand) 0.5)
         (let [[x y] (take 2 (shuffle (range key-count)))]
           [[:r x nil] [:append y (swap! next-value inc)]])
         (let [appended (volatile! #{})]
           (vec
             (repeatedly (+ 2 (rand-int 3))
               (fn []
                 (let [k (rand-int key-count)]
                   (if (or (< (rand) 0.5) (@appended k))
                     [:r k nil]
                     (do (vswap! appended conj k)
                         [:append k (swap! next-value inc)]))))))))})))

(defn final-reads []
  (gen/each-thread
    (map (fn [k]
           (gen/once {:type :invoke :f :txn :final-read true
                      :value [[:r k nil]]}))
         (range key-count))))

(defn coverage-checker
  "An all-aborted run, a disabled SSI flag, or missing final observations must
  never masquerade as a passing serializability test."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [oks (filter #(and (= :ok (:type %)) (= :txn (:f %))) history)
            mixed (count (filter #(let [fs (set (map first (:value %)))]
                                    (and (fs :r) (fs :append))) oks))
            observed (set (for [op (filter :final-read oks)
                                [f k _] (:value op)
                                :when (= :r f)]
                            [(:node op) k]))
            missing (vec (for [node (:nodes test)
                               k (range key-count)
                               :when (not (observed [node k]))]
                           [node k]))
            protocol-errors (vec (filter #(= :ssi-protocol-error (:error %)) history))]
        {:valid? (cond
                   (seq protocol-errors) false
                   (or (zero? mixed) (seq missing)) :unknown
                   :else true)
         :committed-mixed-txns mixed
         :missing-final-read-nodes (vec (distinct (map first missing)))
         :missing-final-reads missing
         :protocol-errors protocol-errors}))))

(defn elle-checker []
  (reify checker/Checker
    (check [_ test history _]
      (elle/check {:consistency-models consistency-models
                   :directory (.getCanonicalPath (store/path! test "elle"))}
                  history))))

(defn workload [{:keys [rate] :or {rate 50}}]
  {:client (map->SSIClient {})
   :generator (gen/stagger (/ 1 rate) (txn-generator))
   :final-generator (final-reads)
   :checker (checker/compose {:elle (elle-checker)
                              :coverage (coverage-checker)})})
