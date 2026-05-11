(ns jepsen.swytch.workload.sql-append
  "Elle list-append workload against swytch's SQL layer.

  Schema:

    CREATE TABLE lists (k INTEGER, v INTEGER, PRIMARY KEY (k, v))

  The composite (k, v) primary key exercises swytch's composite-PK
  tuple encoding and range scans on a BY-v ordered read. Appends
  insert globally-unique v values so repeated reads across a history
  agree on an ordering — Elle's list-append checker consumes that to
  detect cycles (G2, G-single) for a given consistency model.

  Each jepsen op is a transaction of 1-n micro-ops. Multi-op
  transactions get wrapped in BEGIN/COMMIT so elle sees them as a
  single atomic observation."
  (:require [clojure.tools.logging :refer [info warn]]
            [elle.list-append :as ella]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [store :as store]]
            [jepsen.swytch.db :as sdb]
            [jepsen.swytch.sql-client :as sc])
  (:import [java.sql Connection]
           [javax.sql DataSource]))

;; ---- Schema lifecycle ----

(def table-name "lists")

(defn create-schema!
  "CREATE TABLE IF NOT EXISTS — idempotent across setup retries."
  [^DataSource ds]
  (with-open [^Connection c (.getConnection ds)]
    (sc/exec! c
      [(str "CREATE TABLE IF NOT EXISTS " table-name
            " (k INTEGER, v INTEGER, PRIMARY KEY (k, v))")])))

;; ---- Per-op execution ----

(defn exec-read
  "Reads the ordered list for key k. Returns [[:r k vs]]."
  [c [_ k _]]
  (let [rows (sc/query c
               [(str "SELECT v FROM " table-name
                     " WHERE k = ? ORDER BY v") k])
        ;; rows is [[\"v\"] [v1] [v2] ...] with header row when
        ;; builder-fn = as-arrays. Drop the header, unbox the ints.
        vs   (->> rows rest (mapv (comp long first)))]
    [[:r k (or vs [])]]))

(defn exec-append
  "Appends a globally-unique v to the list for key k. Returns
  [[:append k v]]."
  [c [_ k v]]
  (sc/exec! c
    [(str "INSERT INTO " table-name " (k, v) VALUES (?, ?)") k v])
  [[:append k v]])

(defn exec-micro
  "Dispatches a single micro-op."
  [c [f _ _ :as op]]
  (case f
    :r      (exec-read c op)
    :append (exec-append c op)))

(defn exec-txn
  "Executes a jepsen txn (a vec of micro-ops) against the database.
  Single-op txns skip BEGIN/COMMIT since swytch auto-commits. Multi-
  op txns wrap in a transaction; aborts propagate as SQLExceptions
  that the outer with-errors block classifies."
  [^DataSource ds txn]
  (with-open [^Connection c (.getConnection ds)]
    (if (= 1 (count txn))
      (exec-micro c (first txn))
      (sc/with-txn c
        (fn [tx]
          (vec (mapcat #(exec-micro tx %) txn)))))))

;; ---- Client ----

(defrecord AppendClient [ds node schema-ready?]
  client/Client
  (open! [this _test node]
    (assoc this
           :ds   (sc/ds node sdb/sql-port)
           :node node))

  (setup! [this _test]
    ;; Jepsen opens one client per node, so setup! fires five times
    ;; in parallel. The schema only needs to be created once —
    ;; running CREATE TABLE on every node just starts a five-way
    ;; race in the DDL intercept. Compare-and-set on the shared
    ;; atom picks one winner; the rest wait for the winner's commit
    ;; to replicate before returning. A separate sanity select
    ;; ensures the waiters don't proceed until the table is visible
    ;; on their own node.
    (if (compare-and-set! schema-ready? :pending :creating)
      (do
        (create-schema! (:ds this))
        (reset! schema-ready? :ready))
      (loop [tries 60]
        (cond
          (= @schema-ready? :ready) nil
          (zero? tries) (throw (ex-info "schema never became ready"
                                        {:node (:node this)}))
          :else (do (Thread/sleep 500) (recur (dec tries)))))))

  (invoke! [this _test op]
    (sc/with-errors op
      (case (:f op)
        :txn (assoc op
                    :type  :ok
                    :value (exec-txn (:ds this) (:value op))))))

  (teardown! [this _test])

  (close! [this _test]))

(defn sql-append-client []
  (map->AppendClient {:schema-ready? (atom :pending)}))

;; ---- Elle checker wrapper ----

(defn elle-checker
  "Runs elle.list-append/check with the given opts + history and
  writes analysis artefacts into the test's store directory."
  [opts]
  (reify checker/Checker
    (check [_ test history _opts]
      (let [dir (store/path! test "elle")]
        (ella/check (assoc opts :directory (.getCanonicalPath dir))
                    history)))))

;; ---- Workload ----

(def key-count
  "Number of distinct list keys. Tighter keyspace → more contention,
  which is what the serializable checker wants to exercise."
  5)

(def max-txn-length
  "Maximum micro-ops per transaction."
  4)

(let [next-val (atom 0)]
  (defn txn-op
    "Generates a random transaction. Each key is appended to at most
    once per transaction (Elle requires globally unique appends per
    key across the history; uniqueness across the whole history is
    maintained by the monotonic `next-val` counter)."
    [_ _]
    (let [n-ops    (inc (rand-int max-txn-length))
          appended (volatile! #{})]
      {:type  :invoke
       :f     :txn
       :value (vec (repeatedly n-ops
                     (fn []
                       (let [k (rand-int key-count)]
                         (if (or (< (rand) 0.5) (@appended k))
                           [:r k nil]
                           (do (vswap! appended conj k)
                               [:append k (swap! next-val inc)]))))))})))

(defn final-reads
  "A read of every key from every thread, so elle has a full picture
  of the final state."
  []
  (gen/each-thread
    (gen/once
      (fn [_ _]
        {:type  :invoke
         :f     :txn
         :value (mapv (fn [k] [:r k nil]) (range key-count))}))))

(defn workload
  "Returns a workload map. Consistency model defaults to
  :serializable — the strongest level swytch SQL intends to
  provide. Opts:

    :rate                 ops/sec (default 30)
    :consistency-models   list for elle (default [:serializable])"
  [{:keys [rate consistency-models]
    :or   {rate               30
           consistency-models [:serializable]}}]
  {:client          (sql-append-client)
   :generator       (->> txn-op (gen/stagger (/ 1 rate)))
   :final-generator (final-reads)
   :checker         (elle-checker {:consistency-models consistency-models})})
