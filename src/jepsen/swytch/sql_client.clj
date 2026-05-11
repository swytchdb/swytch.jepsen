(ns jepsen.swytch.sql-client
  "JDBC connection helpers for swytch's pg-wire SQL server.

  Swytch SQL is SQLite dialect served over the PostgreSQL wire
  protocol; clients use a pg driver with a SQLite-aware dialect.
  The jepsen harness uses the stock pg JDBC driver — we talk to the
  wire directly (raw SQL, no ORM), and since swytch forces
  simpleQuery mode compatibility via psql-wire, tests pass that mode
  in the connection URL.

  Per-op error handling: jepsen's model is :ok (committed), :fail
  (definitely didn't commit), :info (unknown). We translate the
  pg error classes via SQLState and PSQLException, with serialization
  failure (40001) mapping to :fail since it's a real abort."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.net ConnectException SocketException SocketTimeoutException]
           [java.sql Connection SQLException]
           [java.util.concurrent TimeoutException]
           [org.postgresql.util PSQLException]))

;; ---- Connection spec ----

(defn ds
  "Builds a next.jdbc DataSource for a node's SQL port. `port` is the
  client-facing SQL port (defaults to the swytch SQL default).
  Timeout + protocol options are encoded directly in the JDBC URL
  because next.jdbc's option-map doesn't always propagate pg-specific
  parameters to DriverManager.getConnection (we observed a 60s hang
  where our :connectTimeout value was silently ignored)."
  ([node] (ds node 5433))
  ([node port]
   (let [url (str "jdbc:postgresql://" node ":" port "/swytch"
                  "?preferQueryMode=simple"
                  "&ApplicationName=jepsen.swytch"
                  "&sslmode=disable"
                  ;; pgJDBC connectTimeout is seconds. 10s is plenty for
                  ;; a reachable peer; a silent drop (firewall, route
                  ;; loss) still surfaces inside this bound.
                  "&connectTimeout=10"
                  "&loginTimeout=10"
                  "&socketTimeout=30")]
     (jdbc/get-datasource
       {:jdbcUrl  url
        :user     "jepsen"
        :password "jepsen"}))))

;; ---- Error classification ----

(defn serialization-failure?
  "Returns true when the exception carries a serialization-failure
  SQLState (40001) or an equivalent swytch abort message."
  [^Throwable e]
  (cond
    (instance? PSQLException e)
    (= "40001" (.getSQLState ^PSQLException e))
    (instance? SQLException e)
    (= "40001" (.getSQLState ^SQLException e))
    :else
    (let [msg (.getMessage e)]
      (and msg
           (or (str/includes? msg "serialization failure")
               (str/includes? msg "transaction aborted"))))))

(defn connection-refused?
  "Detects a refused TCP connection — definitely didn't commit."
  [^Throwable e]
  (or (instance? ConnectException e)
      (let [msg (.getMessage e)]
        (and msg (str/includes? (str/lower-case msg) "connection refused")))))

(defmacro with-errors
  "Wraps the body and classifies exceptions into jepsen :fail / :info
  result types. :ok is the caller's responsibility on the success
  path."
  [op & body]
  `(try ~@body
        (catch PSQLException e#
          (cond
            (serialization-failure? e#)
            (assoc ~op :type :fail :error :serialization-failure)

            (connection-refused? e#)
            (assoc ~op :type :fail :error :connection-refused)

            :else
            (assoc ~op :type :info :error (.getMessage e#))))
        (catch SQLException e#
          (if (serialization-failure? e#)
            (assoc ~op :type :fail :error :serialization-failure)
            (assoc ~op :type :info :error (.getMessage e#))))
        (catch ConnectException _#
          (assoc ~op :type :fail :error :connection-refused))
        (catch SocketException e#
          (assoc ~op :type :info :error (.getMessage e#)))
        (catch SocketTimeoutException _#
          (assoc ~op :type :info :error :timeout))
        (catch TimeoutException _#
          (assoc ~op :type :info :error :timeout))))

;; ---- Transaction helpers ----

(defn with-txn
  "Runs body inside an explicit BEGIN/COMMIT/ROLLBACK trio, sending
  each as its own SQL statement.

  We do NOT use jdbc/with-transaction (and its .setAutoCommit/.commit
  path) because swytch's pg-wire layer (psql-wire v0.19.0) hardcodes
  ReadyForQuery status='I', so pgJDBC's PgConnection.commit() and
  rollback() are no-ops — they check the driver's internal
  transactionState and short-circuit when IDLE. The net effect is
  that the client thinks the tx committed while the server silently
  aborts on connection close.

  Sending BEGIN/COMMIT/ROLLBACK as literal SQL bypasses the driver
  transaction machinery entirely: each statement goes through our
  handleBegin/handleCommit/handleRollback intercepts directly, which
  is what the swytch-SQL-as-SQLite-over-pg-wire model wants anyway —
  JDBC autocommit semantics don't map cleanly onto a SQLite dialect,
  and rewiring tx boundaries to explicit SQL mirrors what a native
  SQLite client would do."
  [^Connection c f]
  (jdbc/execute! c ["BEGIN"] {:return-keys false})
  (try
    (let [result (f c)]
      (jdbc/execute! c ["COMMIT"] {:return-keys false})
      result)
    (catch Throwable t
      (try
        (jdbc/execute! c ["ROLLBACK"] {:return-keys false})
        (catch Throwable _))
      (throw t))))

(defn exec!
  "Executes a side-effecting statement. Returns nil."
  [c stmt]
  (jdbc/execute! c stmt {:return-keys false})
  nil)

(defn query
  "Runs a query and returns its rows as a vector of vectors.
  next.jdbc's `as-arrays` builder prepends column names as the
  first element, so `(rest (query ...))` is the data. We keep the
  header for callers that want column names, and let the workload
  code drop it explicitly — less magic at this layer."
  [c stmt]
  (jdbc/execute! c stmt
    {:builder-fn rs/as-arrays
     :return-keys false}))
