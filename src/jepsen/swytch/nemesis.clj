(ns jepsen.swytch.nemesis
  "Nemesis configurations for Swytch Jepsen tests — Phase 1 (single-region).

  Phase 1 models the free-tier nearcache: single region, in-memory,
  no Swytch Cloud. The nemesis injects:
    - Network partitions (majority split, single-node isolation)
    - Single-node kill/restart (process crash + recovery)

  Constraints from design.md:
    - Do NOT kill a node while partitioned — it's a cache, not a
      database, so data on that node is lost forever.
    - Kill/restart runs ONLY during normal operation (before partitions).

  Uses phased scheduling: normal → kill → settle → partition → heal → settle → final-read."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]]
            [jepsen.nemesis.combined :as nc]))

;; ---- Custom partition grudges ----

(defn island-grudge
  "Isolates every node from every other node — N partitions of 1."
  [test]
  (n/complete-grudge (map vector (:nodes test))))

(defn asymmetric-grudge
  "Creates an asymmetric partition: splits nodes into three groups
  A, B, C where A↔B can communicate, B↔C can communicate, but
  A↔C is blocked. B acts as a bridge-like group but neither A nor C
  can reach the other."
  [test]
  (let [nodes   (rand/shuffle (:nodes test))
        n       (count nodes)
        _       (when (< n 3)
                  (throw (ex-info "Asymmetric partition requires at least 3 nodes"
                                  {:node-count n})))
        ;; Split into thirds, ensuring each group has at least 1 node
        a-size  (max 1 (quot n 3))
        b-size  (max 1 (quot n 3))
        c-size  (- n a-size b-size)
        ;; If c would be empty, shrink b to make room
        b-size  (if (pos? c-size) b-size (max 1 (- n a-size 1)))
        a       (set (take a-size nodes))
        b       (set (take b-size (drop a-size nodes)))
        c       (set (drop (+ a-size b-size) nodes))]
    ;; A hates C, C hates A; B is friendly with both
    (merge
      (into {} (for [node a] [node c]))
      (into {} (for [node c] [node a])))))

;; ---- Custom partition specs for combined nemesis ----

(defn island-partition-spec
  "A partition spec that isolates every node."
  [test _db]
  (island-grudge test))

(defn asymmetric-partition-spec
  "A partition spec for asymmetric A↔C blocked, A↔B and B↔C ok."
  [test _db]
  (asymmetric-grudge test))

;; ---- Partition nemesis with custom specs ----

(defn swytch-partition-nemesis
  "A partition nemesis that supports both standard Jepsen specs and
  Swytch-specific ones (:island, :asymmetric)."
  [db]
  (let [p (n/partitioner)]
    (reify
      n/Reflection
      (fs [_] #{:start-partition :stop-partition})

      n/Nemesis
      (setup! [this test]
        (n/setup! p test)
        this)

      (invoke! [_ test op]
        (-> (case (:f op)
              :start-partition
              (let [grudge (case (:value op)
                             :island    (island-grudge test)
                             :asymmetric (asymmetric-grudge test)
                             ;; Fall back to combined.clj's grudge for standard specs
                             (nc/grudge test db (:value op)))]
                (n/invoke! p test (assoc op :f :start :value grudge)))
              :stop-partition
              (n/invoke! p test (assoc op :f :stop)))
            (assoc :f (:f op))))

      (teardown! [_ test]
        (n/teardown! p test)))))

(defn swytch-partition-package
  "Like Jepsen's partition-package but includes Swytch-specific partition types.
  Returns nil nemesis when partitions are not in the fault set, so that
  compose-packages doesn't route partition :f values to this package.

  Partitions are held open for at least 10 seconds so that nodes have
  time to detect the partition and the checker can observe writes on
  both sides."
  [opts]
  (let [needed?  ((:faults opts) :partition)
        targets  (:targets (:partition opts)
                           [:one :majority :majorities-ring])
        start    (fn [_ _] {:type :info
                            :f :start-partition
                            :value (rand/nth targets)})
        stop     {:type :info :f :stop-partition :value nil}
        gen      (gen/cycle-times
                   10 start
                   5  stop)]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (when needed? (swytch-partition-nemesis (:db opts)))
     :perf            (when needed?
                        #{{:name  "partition"
                           :start #{:start-partition}
                           :stop  #{:stop-partition}
                           :color "#E9DCA0"}})}))

;; ---- Composed nemesis packages ----

(defn swytch-nemesis-packages
  "Returns a collection of nemesis packages for the given opts.
  Uses Swytch's custom partition nemesis instead of the default.

  Note: no clock-skew package — swytch doesn't depend on wall-clock
  time, and `ntpdate -b` requires CAP_SYS_TIME which the K8s test
  pods don't have."
  [opts]
  (let [faults (set (:faults opts))
        opts   (assoc opts :faults faults)]
    [(swytch-partition-package opts)
     (nc/db-package opts)]))

(defn nemesis-package
  "Composes all nemesis packages into one. Options:

    :db         The SwytchDB instance
    :faults     Set of enabled faults, e.g. #{:partition :kill}
    :interval   Seconds between nemesis operations (default 10)
    :partition  {:targets [...]}  — partition specs to use
    :kill       {:targets [...]}"
  [opts]
  (nc/compose-packages (swytch-nemesis-packages opts)))

;; ---- Phased generator helpers ----

(defn phase-generator
  "Returns a generator that runs through phases:
    1. Normal operation (no faults) for `normal-secs`
    2. Kill a random node + client ops for a brief window, then heal
    3. Settle after kill for `settle-secs`
    4. Fault injection (partitions) for `fault-secs`
    5. Heal all faults
    6. Settle for `settle-secs` (normal ops, no faults)
    7. Final reads

  `kill-pkg` is a nemesis package for kill/restart (run before partitions).
  `fault-pkg` is a nemesis package for partitions.
  `client-gen` is the generator for client operations.
  Either package may be nil to skip that phase."
  [{:keys [normal-secs fault-secs settle-secs]
    :or   {normal-secs 10
           fault-secs  30
           settle-secs 30}}
   {:keys [kill-pkg fault-pkg]} client-gen final-gen]
  (gen/phases
    ;; Phase 1: normal operation
    (gen/clients
      (gen/time-limit normal-secs client-gen))

    ;; Phase 2: kill a node + client ops (before any partitions)
    (when (:generator kill-pkg)
      (->> client-gen
           (gen/nemesis (:generator kill-pkg))
           (gen/time-limit 10)))

    ;; Phase 3: heal kills
    (when (:final-generator kill-pkg)
      (gen/nemesis (:final-generator kill-pkg)))

    ;; Phase 4: settle after kill
    (when (:generator kill-pkg)
      (gen/clients
        (gen/time-limit settle-secs client-gen)))

    ;; Phase 5: partition faults + client ops
    (when (:generator fault-pkg)
      (->> client-gen
           (gen/nemesis (:generator fault-pkg))
           (gen/time-limit fault-secs)))

    ;; Phase 6: heal faults
    (when (:final-generator fault-pkg)
      (gen/nemesis (:final-generator fault-pkg)))

    ;; Phase 7: settle with writes (normal operation after heal)
    (gen/clients
      (gen/time-limit settle-secs client-gen))

    ;; Phase 8: brief quiet window for anti-entropy
    (gen/sleep 5)

    ;; Phase 9: final reads
    (when final-gen
      (gen/clients final-gen))))

;; ---- Pre-built nemesis configuration ----

(defn safe-nemesis
  "Nemesis package for Phase 1 single-region safe-mode testing.

  Includes:
    - Single-node kill/restart during normal phase (tests crash recovery)
    - Majority/minority partitions during fault phase (tests reachability-based write rules)

  Constraints:
    - Kill happens ONLY before partitions — killing a partitioned cache
      node causes expected data loss (it's a cache, not a database).
    - Kill targets :one only — design.md forbids rebooting the entire
      cluster while partitioned."
  [db]
  {:kill-pkg (nemesis-package
               {:db     db
                :faults #{:kill}
                :kill   {:targets [:one]}})
   :fault-pkg (nemesis-package
                {:db        db
                 :faults    #{:partition}
                 :partition {:targets [:one :majority]}})})

;; ---- Attrition: kills nodes one at a time until one survives, then revives ----
;;
;; Stresses cluster formation under the harshest realistic scenario: a
;; single surviving node must remain consistent through the survivor
;; phase and serve as the sole authoritative source when revived nodes
;; bootstrap. This is the rolling-upgrade pattern that historically
;; triggered ghost-tip propagation bugs in ensureSubscribed.

(def ^:private default-attrition
  {:kill-interval   15   ; secs between sequential kills
   :survivor-secs   60   ; hold with one survivor
   :revive-interval 15}) ; secs between sequential revives

(defn- attrition-schedule
  "Returns [victims-in-kill-order survivor-node] given the test's node
  list. Last node sorted is preserved as the survivor — deterministic
  across runs, which keeps logs and Jepsen replay reproducible."
  [nodes]
  (let [sorted (vec (sort nodes))]
    [(butlast sorted) (last sorted)]))

(defn attrition-generator
  "Generates per-node kill/start ops on the attrition schedule. The
  victim sequence is deterministic (sorted ascending, last node
  reserved as the survivor) so logs and Jepsen history replay are
  reproducible."
  [nodes
   {:keys [kill-interval survivor-secs revive-interval]
    :or   {kill-interval   (:kill-interval default-attrition)
           survivor-secs   (:survivor-secs default-attrition)
           revive-interval (:revive-interval default-attrition)}}]
  (let [[victims survivor] (attrition-schedule nodes)
        kill-ops   (mapcat (fn [v]
                             [{:type :info :f :kill :value [v]}
                              (gen/sleep kill-interval)])
                           victims)
        revive-ops (mapcat (fn [v]
                             [{:type :info :f :start :value [v]}
                              (gen/sleep revive-interval)])
                           victims)]
    (info "attrition: survivor node will be" survivor
          "; victims in kill order:" (vec victims))
    (apply gen/phases
           (concat kill-ops
                   [(gen/sleep survivor-secs)]
                   revive-ops))))

(defn attrition-package
  "Nemesis package shaped like a kill-pkg but carrying the full
  attrition schedule. Reuses jepsen.nemesis.combined/db-nemesis,
  which already knows how to dispatch :kill/:start ops by node spec
  (a literal node list, in our case)."
  [db opts]
  {:nemesis         (nc/db-nemesis db)
   :generator       (attrition-generator (:nodes opts) opts)
   :final-generator {:type :info :f :start :value :all}
   :perf            #{{:name  "kill"
                       :start #{:kill}
                       :stop  #{:start}
                       :color "#E9A4A0"}}})

(defn attrition-nemesis
  "Returns the nemesis-config map for the attrition test. fault-pkg
  is intentionally empty — partitions would muddle the bootstrap
  signal we're trying to isolate."
  [db opts]
  {:kill-pkg  (attrition-package db opts)
   :fault-pkg nil})

(defn attrition-phase-generator
  "Phase orchestration for attrition tests. Skips the standard
  partition phases and gives the attrition schedule enough wall-clock
  to actually fire all its kills + survivor hold + revives. Total
  attrition runtime is N*(kill-interval+revive-interval) +
  survivor-secs; tests should set --time-limit accordingly.

  Phases:
    1. Normal ops for `normal-secs` (warms the cluster, lets bootstrap settle)
    2. Attrition (kills + survivor + revives) running in parallel with client ops
    3. Settle for `settle-secs` (gives anti-entropy / re-bootstrapped nodes time to converge)
    4. Final reads"
  [{:keys [normal-secs settle-secs]
    :or   {normal-secs 20
           settle-secs 30}}
   nemesis-pkg client-gen final-gen]
  (let [kill-pkg (:kill-pkg nemesis-pkg)]
    (gen/phases
      ;; Phase 1: normal ops
      (gen/clients (gen/time-limit normal-secs client-gen))

      ;; Phase 2: attrition + client ops in parallel
      (->> client-gen
           (gen/nemesis (:generator kill-pkg)))

      ;; Phase 3: make sure any still-down nodes get a final start, then settle
      (gen/nemesis (:final-generator kill-pkg))
      (gen/clients (gen/time-limit settle-secs client-gen))

      ;; Phase 4: brief quiet window
      (gen/sleep 5)

      ;; Phase 5: final reads
      (when final-gen
        (gen/clients final-gen)))))
