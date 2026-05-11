(ns jepsen.swytch.db
  "Jepsen DB lifecycle for Swytch: installs the binary, starts/stops
  the chosen transport (redis or sql), and collects logs.

  Cluster bootstrap uses swytch's beacon subsystem: every node starts
  with --cluster-passphrase and --join pointing at a DNS name that
  resolves to the cluster's peer IPs. DNS is managed out-of-band by
  the operator (see :join-dns test option); jepsen does not distribute
  certs or a static topology file."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu])
  (:import [java.io File]))

(def swytch-dir     "/opt/swytch")
(def binary         (str swytch-dir "/swytch"))
(def pid-file       (str swytch-dir "/swytch.pid"))
(def log-file       (str swytch-dir "/swytch.log"))
(def data-dir       (str swytch-dir "/data"))

;; Transport defaults — `:port` is the client-facing listen port,
;; `:cluster-port` is the QUIC port peers dial to each other.
(def transports
  {:redis {:port         6379
           :cluster-port 7379
           :subcommand   "redis"}
   :sql   {:port         5433
           :cluster-port 6433
           :subcommand   "sql"}})

(defn transport-config
  "Resolves transport configuration from a test opts map. :transport
  is either :redis or :sql; defaults to :redis."
  [test]
  (let [t (keyword (or (:transport test) :redis))]
    (or (get transports t)
        (throw (ex-info (str "unknown transport: " t)
                        {:available (keys transports)})))))

;; ---- Build ----

(defn build-swytch!
  "Builds the Swytch binary locally from source. Returns the path."
  [source-dir]
  (let [source-dir (str source-dir)]
    (info "Building Swytch from" source-dir)
    (let [out-path (str source-dir "/swytch")
          env      {"GOOS" "linux" "GOARCH" "amd64" "CGO_ENABLED" "0"}
          run!     (fn [& args]
                     (let [pb ^ProcessBuilder (ProcessBuilder. ^"[Ljava.lang.String;" (into-array String args))
                           pe (.environment pb)]
                       (.directory pb (File. source-dir))
                       (doseq [[k v] env] (.put pe k v))
                       (.redirectErrorStream pb true)
                       (let [p   (.start pb)
                             out (slurp (.getInputStream p))
                             rc  (.waitFor p)]
                         (when (not= 0 rc)
                           (throw (ex-info (str "Build failed: " out)
                                           {:exit rc :output out}))))))]
      (run! "go" "build" "--tags" "nolicense" "-o" out-path ".")
      (info "Build complete:" out-path)
      {:swytch out-path})))

(defn local-md5
  "Returns the md5 hex digest of a local file."
  [path]
  (let [md  (java.security.MessageDigest/getInstance "MD5")
        buf (byte-array 8192)]
    (with-open [is (java.io.FileInputStream. (str path))]
      (loop []
        (let [n (.read is buf)]
          (when (pos? n)
            (.update md buf 0 n)
            (recur)))))
    (apply str (map #(format "%02x" %) (.digest md)))))

(defn install-binary!
  "Stops any running swytch, uploads the binary, and verifies it
  matches the local build via md5."
  [test]
  (c/su
    (cu/stop-daemon! binary pid-file)
    (c/exec :rm :-f binary)
    (c/exec :mkdir :-p swytch-dir)
    (c/exec :mkdir :-p data-dir)
    (c/upload (:swytch-binary test) binary)
    (c/exec :chmod "+x" binary)
    (let [local-hash  (local-md5 (:swytch-binary test))
          remote-hash (first (str/split (c/exec :md5sum binary) #"\s+"))]
      (when (not= local-hash remote-hash)
        (throw (ex-info "Binary mismatch after upload!"
                        {:local  local-hash
                         :remote remote-hash
                         :binary binary}))))))

;; ---- /etc/hosts injection ----

;; Marker lines around a swytch-managed block. Keeps modifications
;; scoped so we can idempotently replace them on restart and strip
;; them on teardown without trashing a hand-written /etc/hosts.
(def hosts-begin-marker "# BEGIN jepsen-swytch")
(def hosts-end-marker   "# END jepsen-swytch")

(defn install-hosts!
  "Writes /etc/hosts entries on this node so `join-dns` resolves to
  every node IP regardless of what the upstream DNS provider returns.
  Some managed-DNS providers truncate A-record responses below the
  record count; libc's /etc/hosts lookup runs before DNS (nsswitch
  `files dns`) so injecting here is the authoritative path.

  Idempotent: strips any prior swytch-managed block before writing."
  [test join-dns]
  (when (and join-dns (not (str/blank? join-dns)))
    (c/su
      (c/exec :sed :-i
              (str "/" hosts-begin-marker "/," "/" hosts-end-marker "/d")
              "/etc/hosts")
      (let [block (str "\n" hosts-begin-marker "\n"
                       (str/join "\n"
                                 (for [ip (sort (:nodes test))]
                                   (str ip " " join-dns)))
                       "\n" hosts-end-marker "\n")]
        (c/exec :bash :-c
                (str "cat >> /etc/hosts <<'HOSTSEOF'\n" block "HOSTSEOF"))))))

(defn uninstall-hosts!
  "Removes the swytch-managed block from /etc/hosts."
  []
  (c/su
    (c/exec :sed :-i
            (str "/" hosts-begin-marker "/," "/" hosts-end-marker "/d")
            "/etc/hosts")))

;; ---- Start / stop ----

(defn resolve-passphrase!
  "Obtains the cluster passphrase. Priority:

   1. --cluster-passphrase on the jepsen CLI (test opts :cluster-passphrase)
   2. Test atom :cluster-passphrase (first node to reach this sets it)
   3. Generate one via `swytch gen-passphrase` and share via atom

  Generation happens on the first node (by sorted name) so every
  other node waits and reads the shared value."
  [test node]
  (or (:cluster-passphrase test)
      (when-let [atm (:cluster-passphrase-atom test)]
        (let [first-node (first (sort (:nodes test)))]
          (if (= node first-node)
            (or @atm
                (let [pass (-> (c/exec binary "gen-passphrase")
                               str/trim)]
                  (info "generated cluster passphrase on" node)
                  (reset! atm pass)
                  pass))
            (loop [tries 60]
              (if-let [pass @atm]
                pass
                (if (zero? tries)
                  (throw (RuntimeException. "timed out waiting for cluster passphrase"))
                  (do (Thread/sleep 1000)
                      (recur (dec tries)))))))))))

(defn start-swytch!
  "Starts the Swytch process on this node. Transport is picked from
  the test's :transport option (default :redis)."
  [test node]
  (let [cfg         (transport-config test)
        subcommand  (:subcommand cfg)
        port        (get-in test [:port-override] (:port cfg))
        cluster-port (:cluster-port cfg)
        passphrase  (resolve-passphrase! test node)
        join-dns    (:join-dns test)
        base-args   (case subcommand
                      "redis"
                      ["redis"
                       "--port"                (str port)
                       "--bind"                "0.0.0.0"
                       "--maxmemory"           "5gb"
                       "--log-format"          "json"
                       "--cluster-passphrase"  passphrase
                       "--cluster-port"        (str cluster-port)]
                      "sql"
                      ["sql"
                       "--listen"              (str "0.0.0.0:" port)
                       "--log-format"          "json"
                       "--cluster-passphrase"  passphrase
                       "--cluster-port"        (str cluster-port)])
        base-args   (cond-> base-args
                      join-dns     (into ["--join" join-dns])
                      (:debug test) (into ["-v"]))]
    (info "Starting Swytch" subcommand "on" node
          "port" port "cluster-port" cluster-port
          "join" join-dns)
    (apply cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   swytch-dir}
      binary
      base-args)))

(defn stop-swytch!
  "Stops the Swytch process."
  []
  (cu/stop-daemon! binary pid-file))

(defn wipe-data!
  "Removes the data dir."
  []
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :mkdir :-p data-dir)))

;; ---- DB lifecycle ----

(defrecord SwytchDB [swytch-binary]
  db/DB
  (setup! [this test node]
    (locking build-swytch!
      (when (and (:swytch-source test)
                 (not (:swytch-binary test))
                 (not @(:built? test)))
        (let [{:keys [swytch]} (build-swytch! (:swytch-source test))]
          (swap! (:test-opts test) assoc :swytch-binary swytch)
          (reset! (:built? test) true))))
    (let [test (if-let [opts @(:test-opts test)]
                 (merge test opts)
                 test)]
      (wipe-data!)
      (c/su (c/exec :rm :-f log-file))
      (install-binary! test)
      (install-hosts! test (:join-dns test))
      (start-swytch! test node)
      ;; Brief sleep so peers have a chance to handshake before we
      ;; accept clients — swytch's session-init path calls
      ;; ensureSubscribed, which refuses to proceed in a "minority
      ;; partition." A few seconds on a LAN is typically enough once
      ;; DNS (via /etc/hosts) resolves to the full member set.
      (Thread/sleep 5000)))

  (teardown! [this test node]
    (stop-swytch!)
    (uninstall-hosts!)
    (wipe-data!))

  db/Kill
  (kill! [this test node]
    (info "Killing Swytch on" node)
    (cu/stop-daemon! binary pid-file))

  (start! [this test node]
    (info "Restarting Swytch on" node)
    (start-swytch! test node))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop binary)))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont binary)))

  db/LogFiles
  (log-files [this test node]
    [log-file]))

(defn db
  "Constructs a SwytchDB instance. Reads :swytch-binary if supplied;
  otherwise the first node that reaches setup! builds it."
  [opts]
  (map->SwytchDB (select-keys opts [:swytch-binary])))

;; ---- Compatibility shims for the existing client code ----

;; The redis client grabbed `sdb/redis-port` as a compile-time
;; constant. Keep it for backwards compatibility even though the
;; transports map is the canonical source.
(def redis-port (get-in transports [:redis :port]))
(def sql-port   (get-in transports [:sql   :port]))
