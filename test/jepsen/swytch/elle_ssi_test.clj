(ns jepsen.swytch.elle-ssi-test
  (:require [clojure.test :refer :all]
            [elle.list-append :as elle]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]]
            [jepsen.generator.context :as context]
            [jepsen.generator.test :as gen-test]
            [jepsen.control.util :as cu]
            [jepsen.swytch :as swytch]
            [jepsen.swytch.db :as db]
            [jepsen.swytch.workload.elle-ssi :as ssi]))

(defn scripted-connection [steps]
  (let [remaining (atom steps)]
    {:command! (fn [command]
                 (let [[expected reply] (first @remaining)]
                   (is (= expected command))
                   (swap! remaining rest)
                   (if (instance? Exception reply) (throw reply) reply)))
     :remaining remaining}))

(deftest interactive-transaction-test
  (let [txn [[:r 0 nil] [:append 1 7] [:r 1 nil]]
        {:keys [command! remaining]}
        (scripted-connection [[["MULTI"] "OK"]
                              [["LRANGE" "ssi-0" 0 -1] []]
                              [["RPUSH" "ssi-1" "7"] 1]
                              [["LRANGE" "ssi-1" 0 -1] ["7"]]
                              [["EXEC"] [[] 1 ["7"]]]])]
    (is (= [[:r 0 []] [:append 1 7] [:r 1 [7]]]
           (ssi/run-txn! command! txn)))
    (is (empty? @remaining))))

(deftest conflict-discards-provisional-reads-test
  (let [{:keys [command! remaining]}
        (scripted-connection [[["MULTI"] "OK"]
                              [["LRANGE" "ssi-0" 0 -1] ["9"]]
                              [["RPUSH" "ssi-1" "7"] 1]
                              [["EXEC"] nil]])]
    (is (nil? (ssi/run-txn! command! [[:r 0 nil] [:append 1 7]])))
    (is (empty? @remaining))))

(deftest ordinary-final-read-test
  (doseq [reply [[] ["7" "9"]]]
    (let [{:keys [command! remaining]}
          (scripted-connection [[["LRANGE" "ssi-2" 0 -1] reply]])]
      (is (= [[:r 2 (mapv parse-long reply)]]
             (ssi/run-final-read! command! [[:r 2 nil]])))
      (is (empty? @remaining))))
  (testing "independent reads cannot be represented as one atomic operation"
    (doseq [txn [[[:r 0 nil] [:r 1 nil]] [[:append 0 1]]]]
      (is (thrown? clojure.lang.ExceptionInfo
            (ssi/run-final-read! (fn [_] (throw (AssertionError. "unexpected command")))
                                 txn)))))
  (testing "malformed replies cannot count as successful observations"
    (doseq [reply [nil "QUEUED" ["not-an-integer"]]]
      (is (thrown? clojure.lang.ExceptionInfo
            (ssi/run-final-read! (constantly reply) [[:r 0 nil]]))))))

(deftest final-read-dispatch-test
  (let [c (ssi/map->SSIClient {:node "n1"})
        op {:type :invoke :f :txn :final-read true :value [[:r 2 nil]]}]
    (with-redefs [ssi/exec-txn (fn [& _] (throw (AssertionError. "unexpected transaction")))
                  ssi/exec-final-read (fn [_ txn]
                                       (is (= [[:r 2 nil]] txn))
                                       [[:r 2 [7]]])]
      (is (= (assoc op :type :ok :node "n1" :value [[:r 2 [7]]])
             (client/invoke! c {} op))))
    (with-redefs [ssi/exec-final-read (fn [& _] (throw (java.io.EOFException. "closed")))]
      (let [result (client/invoke! c {} op)]
        (is (= :info (:type result)))
        (is (= (:value op) (:value result)))))))

(deftest reject-queued-and-replayed-results-test
  (doseq [steps [[[["MULTI"] "OK"]
                 [["LRANGE" "ssi-0" 0 -1] "QUEUED"]]
                [[["MULTI"] "OK"]
                 [["LRANGE" "ssi-0" 0 -1] []]
                 [["EXEC"] [["1"]]]]
                [[["MULTI"] "OK"]
                 [["LRANGE" "ssi-0" 0 -1] []]
                 [["EXEC"] []]]]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unexpected SSI reply"
          (ssi/run-txn! (:command! (scripted-connection steps)) [[:r 0 nil]])))))

(deftest operation-outcome-test
  (let [c (ssi/map->SSIClient {:node "n1"})
        op {:type :invoke :f :txn :value [[:r 0 nil] [:append 1 7]]}]
    (testing "a null EXEC is a definite abort"
      (with-redefs [ssi/exec-txn (fn [& _] nil)]
        (let [result (client/invoke! c {} op)]
          (is (= :fail (:type result)))
          (is (= :ssi-conflict (:error result)))
          (is (= (:value op) (:value result))))))
    (testing "lost replies and Carmine-wrapped connection failures are uncertain"
      (doseq [error [(java.net.SocketTimeoutException. "lost EXEC")
                     (java.io.EOFException. "closed")
                     (ex-info "Carmine connection error" {})]]
        (with-redefs [ssi/exec-txn (fn [& _] (throw error))]
          (let [result (client/invoke! c {} op)]
            (is (= :info (:type result)))
            (is (= (:value op) (:value result)))))))
    (testing "protocol mismatches are visible to the checker"
      (with-redefs [ssi/exec-txn (fn [& _]
                                 (throw (ex-info "QUEUED" {:type :ssi-protocol-error})))]
        (is (= :ssi-protocol-error (:error (client/invoke! c {} op))))))))

(defn history
  "Pairs concurrent invocations with completed transactions."
  [txns]
  (h/history
    (vec (concat
           (map-indexed (fn [p txn]
                          {:process p :type :invoke :f :txn
                           :value (mapv (fn [[f k v]] [f k (when (= :append f) v)]) txn)})
                        txns)
           (map-indexed (fn [p txn]
                          {:process p :type :ok :f :txn :value txn}) txns)))))

(deftest elle-distinguishes-si-from-serializable-test
  (let [write-skew (history [[[:r 0 []] [:append 1 1]]
                             [[:r 1 []] [:append 0 2]]])
        serial (history [[[:r 0 []] [:append 1 1]]
                         [[:r 1 [1]] [:append 0 2]]])]
    (testing "write skew is permitted by SI but rejected by this workload"
      (is (true? (:valid? (elle/check {:consistency-models [:snapshot-isolation]}
                                     write-skew))))
      (is (false? (:valid? (elle/check {:consistency-models ssi/consistency-models}
                                      write-skew)))))
    (testing "a serializable history passes"
      (is (true? (:valid? (elle/check {:consistency-models ssi/consistency-models}
                                     serial)))))))

(deftest aborted-reads-test
  (testing "reading an aborted append is rejected"
    (let [hist (h/history [{:process 0 :type :invoke :f :txn :value [[:append 0 1]]}
                          {:process 0 :type :fail :f :txn :value [[:append 0 1]]}
                          {:process 1 :type :invoke :f :txn :value [[:r 0 nil]]}
                          {:process 1 :type :ok :f :txn :value [[:r 0 [1]]]}])]
      (is (false? (:valid? (elle/check {:consistency-models ssi/consistency-models} hist)))))))

(deftest coverage-test
  (let [c (ssi/coverage-checker)
        test {:nodes ["n1" "n2"]}
        mixed {:type :ok :f :txn :value [[:r 0 []] [:append 1 1]]}
        finals (vec (for [node (:nodes test) k (range ssi/key-count)]
                      {:type :ok :f :txn :final-read true
                       :node node :value [[:r k []]]}))
        complete (conj finals mixed)]
    (is (= :unknown (:valid? (checker/check c test [] {}))))
    (is (= :unknown (:valid? (checker/check c test [mixed] {}))))
    (is (true? (:valid? (checker/check c test complete {}))))
    (is (= :unknown (:valid? (checker/check c test finals {}))))
    (testing "every key on every node must have a successful final read"
      (doseq [outcome [nil :fail :info]]
        (let [partial (cond-> (subvec complete 1)
                        outcome (conj (assoc (first finals) :type outcome)))
              result (checker/check c test partial {})]
          (is (= :unknown (:valid? result)))
          (is (= [["n1" 0]] (:missing-final-reads result)))
          (is (= ["n1"] (:missing-final-read-nodes result))))))
    (is (false? (:valid? (checker/check c test
                          (conj complete {:type :info :error :ssi-protocol-error}) {}))))))

(deftest final-read-generator-test
  (let [ops (gen-test/perfect (context/context {:concurrency 5})
                             (gen/clients (ssi/final-reads)))]
    (is (= (* 5 ssi/key-count) (count ops)))
    (doseq [p (range 5)]
      (let [reads (filter #(= p (:process %)) ops)]
        (is (every? :final-read reads))
        (is (every? #(= :txn (:f %)) reads))
        (is (= (mapv (fn [k] [[:r k nil]]) (range ssi/key-count))
               (mapv :value reads)))))))

(deftest generator-test
  (let [g (ssi/txn-generator)
        txns (mapv (fn [_] (:value (g nil nil))) (range 1000))
        appends (filter #(= :append (first %)) (mapcat identity txns))
        values (map #(nth % 2) appends)]
    (is (every? #(<= 2 (count %) 4) txns))
    (is (= (count values) (count (set values))))
    (is (every? #(<= 0 (second %) (dec ssi/key-count)) (mapcat identity txns))))
  (with-redefs [rand (constantly 0)]
    (let [[r a] (:value ((ssi/txn-generator) nil nil))]
      (is (= :r (first r)))
      (is (= :append (first a)))
      (is (not= (second r) (second a))))))

(deftest workload-enables-ssi-test
  (doseq [[workload enabled?] [["elle-ssi" true] ["elle-causal" false]
                              ["counter" false] ["sql-append" false]]]
    (let [test (swytch/swytch-test {:workload workload :nodes ["n1"]
                                   :nemesis-config "none"})
          args (atom nil)]
      (is (= enabled? (:ssi test)))
      (with-redefs [db/resolve-passphrase! (fn [& _] "test-passphrase")
                    cu/start-daemon! (fn [& xs] (reset! args xs))]
        (db/start-swytch! test "n1")
        (is (= enabled? (boolean (some #{"--ssi"} @args))))))))
