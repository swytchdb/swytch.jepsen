(ns jepsen.swytch
  "Entry point for the Jepsen test suite for Swytch."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.swytch.checker :as sc]
            [jepsen.swytch.client :as swytch-client]
            [jepsen.swytch.db :as sdb]
            [jepsen.swytch.nemesis :as sn]
            [jepsen.swytch.os :as swytch-os]
            [jepsen.swytch.workload.counter :as counter]
            [jepsen.swytch.workload.elle-causal :as elle-causal]
            [jepsen.swytch.workload.set :as set-wl]
            [jepsen.swytch.workload.sorted-set :as sorted-set]
            [jepsen.swytch.workload.sql-append :as sql-append]))

(def workloads
  "Map of workload names to {:transport :fn}. :transport tells the
  DB layer which swytch subcommand to launch. Redis and SQL
  workloads cannot share a cluster within one run — they're
  different front-ends to the same storage model, so tests pick one."
  {:counter      {:transport :redis :fn counter/workload}
   :set          {:transport :redis :fn set-wl/workload}
   :sorted-set   {:transport :redis :fn sorted-set/workload}
   :elle-causal  {:transport :redis :fn elle-causal/workload}
   :sql-append   {:transport :sql   :fn sql-append/workload}})

(def nemesis-configs
  "Map of nemesis configuration names to constructor functions.

  :none      — no faults; baseline.
  :safe      — single-node kill/restart followed by partitions.
  :attrition — sequentially kill nodes until one survives, hold, then
               revive. Stresses bootstrap with a single authoritative
               source. The constructor receives `opts` (not just `db`)
               because the schedule consults `:nodes` at op-emission
               time. swytch-test below routes attrition through an
               alternative phase generator."
  {:none        (fn [_db _opts] {:kill-pkg  nil
                                 :fault-pkg {:nemesis         nemesis/noop
                                             :generator       nil
                                             :final-generator nil
                                             :perf            #{}}})
   :safe        (fn [db _opts] (sn/safe-nemesis db))
   :attrition   (fn [db  opts] (sn/attrition-nemesis db opts))})

(defn swytch-test
  "Constructs a Jepsen test map for Swytch. The chosen workload
  determines the transport (redis or sql) the DB layer will start
  on each node."
  [opts]
  (let [built?                  (atom false)
        test-opts               (atom nil)
        cluster-passphrase-atom (atom (:cluster-passphrase opts))
        workload-name           (keyword (:workload opts "counter"))
        workload-entry          (get workloads workload-name)
        _                       (when-not workload-entry
                                  (throw (ex-info (str "Unknown workload: " workload-name)
                                                 {:available (keys workloads)})))
        transport               (:transport workload-entry)
        workload-fn             (:fn workload-entry)
        opts                    (assoc opts :transport transport)
        db                      (sdb/db opts)
        workload                (workload-fn opts)
        nemesis-name    (keyword (:nemesis-config opts "safe"))
        nemesis-fn      (get nemesis-configs nemesis-name)
        _               (when-not nemesis-fn
                          (throw (ex-info (str "Unknown nemesis config: " nemesis-name)
                                         {:available (keys nemesis-configs)})))
        nemesis-pkg     (nemesis-fn db opts)
        ;; safe-nemesis returns {:kill-pkg ... :fault-pkg ...}
        ;; :none returns a simple package — wrap it for compatibility
        kill-pkg        (:kill-pkg nemesis-pkg)
        fault-pkg       (:fault-pkg nemesis-pkg)
        ;; Compose nemeses from both packages into one.
        ;; nemesis/compose with a map expects set keys: {#{:f1 :f2} nemesis}
        ;; nemesis/noop doesn't support Reflection (fs), so guard against that.
        fs-or-empty (fn [nem] (if nem (try (nemesis/fs nem) (catch AbstractMethodError _ #{})) #{}))
        combined-nemesis (nemesis/compose
                           (into {}
                             (keep identity
                               [(when-let [fs (not-empty (fs-or-empty (:nemesis kill-pkg)))]
                                  [fs (:nemesis kill-pkg)])
                                (when-let [fs (not-empty (fs-or-empty (:nemesis fault-pkg)))]
                                  [fs (:nemesis fault-pkg)])])))]
    (merge tests/noop-test
           opts
           {:name                    (str "swytch-" (name workload-name))
            :os                      swytch-os/os
            :db                      db
            :transport               transport
            :built?                  built?
            :test-opts               test-opts
            :cluster-passphrase-atom cluster-passphrase-atom
            :client                  (or (:client workload)
                                         (case transport
                                           :redis (swytch-client/client)
                                           :sql   (throw (ex-info
                                                          "SQL workloads must provide their own :client"
                                                          {:workload workload-name}))))
            :nemesis        combined-nemesis
            :checker        (checker/compose
                              (merge
                                {:perf                 (checker/perf)
                                 :timeline             (timeline/html)
                                 :workload             (:checker workload)
                                 :availability         (sc/availability-checker)}
                                (when (not= nemesis-name :none)
                                  {:partition-effective (sc/partition-effective-checker)})))
            :generator      (if (= nemesis-name :attrition)
                              (sn/attrition-phase-generator
                                {:normal-secs (:normal-secs opts 20)
                                 :settle-secs (:settle-secs opts 30)}
                                nemesis-pkg
                                (:generator workload)
                                (:final-generator workload))
                              (sn/phase-generator
                                {:normal-secs (:normal-secs opts 10)
                                 :fault-secs  (:fault-secs opts 30)
                                 :settle-secs (:settle-secs opts 30)}
                                nemesis-pkg
                                (:generator workload)
                                (:final-generator workload)))
            :perf           (into #{} (concat (:perf kill-pkg) (:perf fault-pkg)))})))

;; ---- CLI ----

(def cli-opts
  "Additional CLI options for Swytch tests."
  [[nil "--swytch-source PATH" "Path to Swytch source directory (builds automatically)"
    :default "../swytch"]
   [nil "--swytch-binary PATH" "Path to a pre-built Swytch binary (skips build)"
    :default nil]
   [nil "--workload NAME" "Workload to run: counter, set, sorted-set, elle-causal, sql-append"
    :default "counter"]
   [nil "--nemesis-config NAME" "Nemesis config: none, safe, attrition"
    :default "safe"]
   [nil "--join-dns DNSNAME" "DNS name peers resolve to discover each other. Operator-managed."
    :default nil]
   [nil "--cluster-passphrase PASS" "Shared passphrase for cluster mTLS. When unset, the test generates one and shares it across nodes."
    :default nil]
   [nil "--rate NUM" "Ops per second"
    :default 100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--normal-secs NUM" "Seconds of normal operation before faults"
    :default 10
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--fault-secs NUM" "Seconds of fault injection"
    :default 30
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--settle-secs NUM" "Seconds to settle after healing (needs time for anti-entropy)"
    :default 30
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--debug" "Enable debug logging on Swytch nodes"
    :default false]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   swytch-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
