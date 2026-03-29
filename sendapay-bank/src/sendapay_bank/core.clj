(ns sendapay-bank.core
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [jepsen [checker :as checker]
             [client :as client]
             [control :as control]
             [core :as jepsen]
             [generator :as gen]
             [nemesis :as nemesis]
             [store :as store]
             [tests :as tests]]
            [jepsen.tests.bank :as bank]))

(def ^:private default-database
  "sendapay_jepsen_bank")

(def ^:private default-local-sendapay-root
  "/Users/mac/Desktop/repos/sendapay-backend")

(def ^:private default-docker-sendapay-root
  "/workspace/sendapay-backend")

(def ^:private default-docker-app-nodes
  ["n1" "n3"])

(def ^:private default-shared-root
  "/var/jepsen/shared")

(def ^:private default-docker-private-key-path
  "/root/.ssh/id_rsa.pem")

(def ^:private default-remote-sendapay-root
  (str default-shared-root "/sendapay-backend"))

(def ^:private default-remote-helper-root
  (str default-shared-root "/sendapay-bank"))

(def ^:private default-remote-log-root
  (str default-remote-helper-root "/logs"))

(def ^:private default-local-db-url
  (str "postgresql+psycopg://sendapay:sendapay@127.0.0.1:15432/"
       default-database
       "?connect_timeout=5"))

(def ^:private default-docker-state-file
  (str default-remote-helper-root "/state/sendapay-bank-state.json"))

(def ^:private default-postgres-log-path
  "/var/log/postgresql/postgresql-15-main.log")

(def ^:private default-nemesis-warmup-s
  5.0)

(def ^:private cli-options
  [[nil "--topology MODE" "Run the helper locally or via the Jepsen Docker app/db nodes"
    :default "local"
    :validate [#(#{"local" "docker"} %) "must be local or docker"]]
   ["-r" "--sendapay-root PATH" "Path to the Sendapay backend repo visible to the control process"
    :default default-local-sendapay-root]
   [nil "--db-url URL" "Benchmark database URL"
    :default default-local-db-url]
   [nil "--app-node HOST" "Single Docker topology app/helper node (compatibility flag; prefer --app-nodes)"]
   [nil "--app-nodes HOSTS" "Comma-separated Docker topology app/helper nodes"]
   [nil "--db-node HOST" "Docker topology Postgres node"
    :default "n2"]
   [nil "--ssh-user USER" "SSH user for Docker topology"
    :default "root"]
   [nil "--ssh-password PASS" "SSH password for Docker topology"
    :default "root"]
   [nil "--ssh-private-key-path PATH" "SSH identity file for Docker topology"
    :default default-docker-private-key-path]
   [nil "--remote-sendapay-root PATH" "Shared Sendapay app path on Docker nodes"
    :default default-remote-sendapay-root]
   [nil "--remote-helper-root PATH" "Shared helper staging path on Docker nodes"
    :default default-remote-helper-root]
   [nil "--state-file PATH" "Path to the helper state file"
    :default nil]
   ["-n" "--account-count N" "Number of Sendapay wallet accounts to track"
    :default 8
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "must be positive"]]
   [nil "--initial-balance-cents N" "Initial cents to mint into each tracked user wallet"
    :default 100000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--max-transfer-cents N" "Largest random transfer amount, in cents"
    :default 2500
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   ["-c" "--concurrency N" "Number of Jepsen worker processes"
    :default 4
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "must be positive"]]
   ["-t" "--time-limit S" "Duration of the Jepsen run, in seconds"
    :default 15
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--stagger-ms N" "Average delay between operations per worker"
    :default 50
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "must be non-negative"]]
   [nil "--user-start-id N" "Base user id for provisioned Jepsen actors"
    :default 9800000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--phone-prefix PREFIX" "E.164 suffix prefix for Jepsen actors"
    :default "744"]
   [nil "--pool-size N" "SQLAlchemy pool size for the helper app"
    :default 12
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "must be positive"]]
   [nil "--max-overflow N" "SQLAlchemy max_overflow for the helper app"
    :default 24
    :parse-fn #(Integer/parseInt %)
    :validate [(complement neg?) "must be non-negative"]]
   [nil "--nemesis MODE" "Fault mode for Docker topology"
    :default "none"
    :validate [#(#{"none" "partition-app-db" "restart-db"} %)
               "must be none, partition-app-db, or restart-db"]]
   [nil "--nemesis-interval-s S" "Average seconds between nemesis transitions"
    :default 8.0
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "must be positive"]]
   [nil "--append-only" "Enable Sendapay append-only confirm feature flags"
    :default false]
   ["-h" "--help" "Show help"]])

(defn- project-root []
  (.getCanonicalPath (io/file (System/getProperty "user.dir"))))

(defn- helper-script-path []
  (.getCanonicalPath (io/file (project-root) "python" "sendapay_bank_ops.py")))

(defn- docker-db-url
  [db-node]
  (str "postgresql+psycopg://sendapay:sendapay@"
       db-node
       ":5432/"
       default-database
       "?connect_timeout=5"))

(defn- remote-log-root
  [opts]
  (or (:remote-log-root opts)
      default-remote-log-root))

(defn- helper-log-path
  [opts node]
  (str (remote-log-root opts) "/" node "-helper.log"))

(defn- artifact-path!
  [test checker-opts & parts]
  (apply store/path! test (:subdirectory checker-opts) "artifacts" parts))

(defn- docker-topology?
  [options]
  (= "docker" (:topology options)))

(defn- ensure-parent!
  [path]
  (let [f (io/file path)
        parent (.getParentFile f)]
    (when parent
      (.mkdirs parent))
    (.getCanonicalPath f)))

(defn- canonical-path!
  [path label]
  (let [f (io/file path)]
    (when-not (.exists f)
      (throw (ex-info (str label " not found")
                      {:path path})))
    (.getCanonicalPath f)))

(defn- parse-host-list
  [raw]
  (if raw
    (->> (str/split raw #",")
         (map str/trim)
         (remove str/blank?)
         vec)
    []))

(defn- selected-app-nodes
  [options]
  (let [app-nodes (parse-host-list (:app-nodes options))
        app-node  (some-> (:app-node options) str/trim)]
    (cond
      (seq app-nodes) app-nodes
      (and app-node (not (str/blank? app-node))) [app-node]
      (docker-topology? options) default-docker-app-nodes
      :else ["local"])))

(defn- normalized-options
  [options]
  (let [docker? (docker-topology? options)
        app-nodes (vec (distinct (selected-app-nodes options)))
        _ (when (empty? app-nodes)
            (throw (ex-info "At least one app node is required"
                            {:options options})))
        _ (when (and docker?
                     (some #{(:db-node options)} app-nodes))
            (throw (ex-info "App nodes must not include the DB node"
                            {:app-nodes app-nodes
                             :db-node (:db-node options)})))
        requested-root (:sendapay-root options)
        sendapay-root (cond
                        (and docker?
                             (not (.exists (io/file requested-root)))
                             (.exists (io/file default-docker-sendapay-root)))
                        (canonical-path! default-docker-sendapay-root "Docker-mounted Sendapay root")

                        :else
                        (canonical-path! requested-root "Sendapay root"))
        state-file (if docker?
                     (or (:state-file options)
                         default-docker-state-file)
                     (ensure-parent!
                       (or (:state-file options)
                           (.getPath (io/file (project-root) "target" "sendapay-bank-state.json")))))
        remote-helper-root (:remote-helper-root options)
        db-url (if (and docker?
                        (= default-local-db-url (:db-url options)))
                 (docker-db-url (:db-node options))
                 (:db-url options))]
    (assoc options
           :app-nodes app-nodes
           :primary-app-node (first app-nodes)
           :cluster-nodes (if docker?
                            (vec (distinct (concat app-nodes [(:db-node options)])))
                            ["local"])
           :sendapay-root sendapay-root
           :db-url db-url
           :state-file state-file
           :remote-log-root (remote-log-root options)
           :remote-helper-path (str remote-helper-root "/sendapay_bank_ops.py"))))

(defn- python-executable
  [sendapay-root]
  (let [venv-python (io/file sendapay-root ".venv" "bin" "python")]
    (if (.exists venv-python)
      (.getCanonicalPath venv-python)
      "python3")))

(defn- remote-python-executable
  [opts]
  (str (:remote-sendapay-root opts) "/.venv/bin/python"))

(defn- shell-quote
  [s]
  (str "'" (str/replace (str s) "'" "'\"'\"'") "'"))

(defn- run-local-command!
  [label & cmd]
  (let [{:keys [exit out err]} (apply sh cmd)]
    (when-not (zero? exit)
      (throw (ex-info label
                      {:cmd cmd
                       :exit exit
                       :stderr err
                       :stdout out})))
    {:out out
     :err err}))

(defn- run-bash!
  [label script]
  (run-local-command! label "bash" "-lc" script))

(defn- ssh-target
  [opts node]
  (str (:ssh-user opts) "@" node))

(defn- remote-bash!
  [opts node script]
  (let [ssh-script (str "ssh -o StrictHostKeyChecking=no "
                        "-o UserKnownHostsFile=/dev/null "
                        (shell-quote (ssh-target opts node))
                        " "
                        (shell-quote (str "bash -lc " (shell-quote script))))]
    (run-bash! (str "remote command failed on " node) ssh-script)))

(def ^:private app-runtime-packages
  ["python3-venv"
   "python3-pip"
   "postgresql-client"
   "build-essential"
   "libpq-dev"
   "libjpeg62-turbo-dev"
   "zlib1g-dev"
   "libfreetype6-dev"
   "libffi-dev"
   "libssl-dev"
   "pkg-config"])

(defn- ensure-docker-context!
  [opts]
  (when-not (= "docker" (System/getenv "container"))
    (throw (ex-info "Docker topology must run inside the jepsen-control container"
                    {:hint "Use docker exec jepsen-control bash -lc 'cd /jepsen/sendapay-bank && lein run -- --topology docker ...'"})))
  (when-not (.exists (io/file default-shared-root))
    (throw (ex-info "Expected Jepsen shared volume is missing"
                    {:path default-shared-root})))
  (when-not (.exists (io/file (:sendapay-root opts)))
    (throw (ex-info "Mounted Sendapay source repo is missing in control container"
                    {:path (:sendapay-root opts)}))))

(defn- stage-docker-sources!
  [opts]
  (let [sendapay-root (:sendapay-root opts)
        remote-sendapay-root (:remote-sendapay-root opts)
        remote-helper-root (:remote-helper-root opts)
        remote-log-root (:remote-log-root opts)
        helper-script (:remote-helper-path opts)
        state-parent (.getParent (io/file (:state-file opts)))]
    (run-bash!
      "failed to stage Sendapay sources into the shared Jepsen volume"
      (str/join
        "\n"
        [(str "set -euo pipefail")
         (str "mkdir -p "
              (shell-quote remote-sendapay-root)
              " "
              (shell-quote remote-helper-root)
              " "
              (shell-quote remote-log-root)
              " "
              (shell-quote state-parent))
         (str "tar --exclude='.git' --exclude='.venv' --exclude='__pycache__' "
              "--exclude='.pytest_cache' --exclude='.mypy_cache' "
              "-C "
              (shell-quote sendapay-root)
              " -czf - . | tar -xzf - -C "
              (shell-quote remote-sendapay-root))
         (str "install -m 755 "
              (shell-quote (helper-script-path))
              " "
              (shell-quote helper-script))]))))

(defn- ensure-db-node!
  [opts]
  (let [db-node (:db-node opts)
        postgres-conf "/etc/postgresql/15/main/postgresql.conf"
        pg-hba-conf "/etc/postgresql/15/main/pg_hba.conf"]
    (remote-bash!
      opts
      db-node
      (str/join
        "\n"
        ["set -euo pipefail"
         "export DEBIAN_FRONTEND=noninteractive"
         "apt-get update"
         "apt-get install -y postgresql postgresql-client"
         "systemctl enable postgresql"
         "systemctl start postgresql"
         (str "conf=" (shell-quote postgres-conf))
         (str "hba=" (shell-quote pg-hba-conf))
         "if grep -qE '^#?listen_addresses\\s*=' \"$conf\"; then"
         "  sed -i \"s|^#\\?listen_addresses\\s*=.*|listen_addresses = '*'|\" \"$conf\""
         "else"
         "  printf \"\\nlisten_addresses = '*'\\n\" >> \"$conf\""
         "fi"
         "grep -q 'host all all 0.0.0.0/0 scram-sha-256' \"$hba\" || printf '\\nhost all all 0.0.0.0/0 scram-sha-256\\n' >> \"$hba\""
         "systemctl restart postgresql"
         "su - postgres -c \"psql -v ON_ERROR_STOP=1 <<'SQL'\nDO \\$\\$\nBEGIN\n  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sendapay') THEN\n    CREATE ROLE sendapay LOGIN PASSWORD 'sendapay' CREATEDB;\n  ELSE\n    ALTER ROLE sendapay WITH LOGIN PASSWORD 'sendapay' CREATEDB;\n  END IF;\nEND\n\\$\\$;\nSQL\""
         "pg_isready -h 127.0.0.1 -p 5432"]))))

(defn- ensure-app-node-runtime!
  [opts app-node]
  (let [app-node app-node
        remote-sendapay-root (:remote-sendapay-root opts)
        package-list (str/join " " app-runtime-packages)]
    (remote-bash!
      opts
      app-node
      (str/join
        "\n"
        ["set -euo pipefail"
         "export DEBIAN_FRONTEND=noninteractive"
         "export PIP_DISABLE_PIP_VERSION_CHECK=1"
         "apt-get update"
         (str "apt-get install -y " package-list)
         (str "cd " (shell-quote remote-sendapay-root))
         "req_hash=$(sha256sum requirements.txt | awk '{print $1}')"
         "marker=.venv/.jepsen-requirements.sha256"
         "if [ ! -x .venv/bin/python ] || [ ! -f \"$marker\" ] || [ \"$(cat \"$marker\")\" != \"$req_hash\" ]; then"
         "  python3 -m venv .venv"
         "  .venv/bin/pip install --upgrade pip"
         "  .venv/bin/pip install -r requirements.txt"
         "  printf '%s' \"$req_hash\" > \"$marker\""
         "fi"
         (str "PGPASSWORD=sendapay psql "
              (shell-quote (str "postgresql://sendapay:sendapay@" (:db-node opts) ":5432/postgres"))
              " -c 'SELECT 1' >/dev/null")]))))

(defn- ensure-app-node-runtimes!
  [opts]
  (doseq [app-node (:app-nodes opts)]
    (ensure-app-node-runtime! opts app-node)))

(defn- ensure-docker-topology!
  [opts]
  (ensure-docker-context! opts)
  (stage-docker-sources! opts)
  (ensure-db-node! opts)
  (ensure-app-node-runtimes! opts))

(defn- reset-docker-logs!
  [opts]
  (let [remote-log-root (:remote-log-root opts)]
    (doseq [app-node (:app-nodes opts)]
      (remote-bash!
        opts
        app-node
        (str/join
          "\n"
          ["set -euo pipefail"
           (str "mkdir -p " (shell-quote remote-log-root))
           (str ": > " (shell-quote (helper-log-path opts app-node)))])))
    (remote-bash!
      opts
      (:db-node opts)
      (str/join
        "\n"
        ["set -euo pipefail"
         (str "log_path=" (shell-quote default-postgres-log-path))
         "mkdir -p /var/log/postgresql"
         "if [ -f \"$log_path\" ]; then"
         (str "  su postgres -s /bin/sh -c "
              (shell-quote (str "truncate -s 0 " default-postgres-log-path)))
         "else"
         "  install -o postgres -g adm -m 640 /dev/null \"$log_path\""
         "fi"]))))

(defn- parse-json
  [raw]
  (json/parse-string raw false))

(defn- parse-helper-response
  [out err]
  (let [combined (str out "\n" err)
        json-line (some (fn [line]
                          (let [trimmed (str/trim line)]
                            (when (str/starts-with? trimmed "{")
                              trimmed)))
                        (reverse (str/split-lines combined)))]
    (when-not json-line
      (throw (ex-info "Sendapay helper did not emit JSON"
                      {:stdout out
                       :stderr err})))
    (parse-json json-line)))

(defn- helper-target-node
  [opts target-node]
  (or target-node
      (:primary-app-node opts)
      (first (:app-nodes opts))))

(defn- helper-call-on!
  [opts target-node & args]
  (if (docker-topology? opts)
    (let [app-node (helper-target-node opts target-node)
          log-path (helper-log-path opts app-node)
          invocation (str/join " " (map str (remove nil? args)))
          cmd (str "cd "
                   (shell-quote (:remote-sendapay-root opts))
                   " && "
                   (str/join
                     " "
                     (map shell-quote
                          (into [(remote-python-executable opts)
                                 (:remote-helper-path opts)]
                                (remove nil? args)))))
          script (str/join
                   "\n"
                   ["set -euo pipefail"
                    (str "mkdir -p " (shell-quote (:remote-log-root opts)))
                    (str "log_path=" (shell-quote log-path))
                    (str "invocation=" (shell-quote invocation))
                    "printf '\\n=== %s %s ===\\n' \"$(date -u +%FT%TZ)\" \"$invocation\" >> \"$log_path\""
                    (str cmd " 2>&1 | tee -a \"$log_path\"")])
          {:keys [out err]} (remote-bash! opts app-node script)]
      (parse-helper-response out err))
    (let [cmd (into [(python-executable (:sendapay-root opts))
                     (helper-script-path)]
                    (remove nil? args))
          {:keys [out err]} (apply run-local-command! "Sendapay helper failed" cmd)]
      (parse-helper-response out err))))

(defn- helper-call!
  [opts & args]
  (apply helper-call-on! opts nil args))

(defn- transfer-op
  [test _]
  (let [accounts (:transfer-accounts test)]
    {:type :invoke
     :f :transfer
     :value {:from (rand-nth accounts)
             :to (rand-nth accounts)
             :amount (inc (rand-int (int (:max-transfer test))))}}))

(def ^:private diff-transfer
  (gen/filter (fn [op]
                (not= (get-in op [:value :from])
                      (get-in op [:value :to])))
              transfer-op))

(defn- read-op
  [_ _]
  {:type :invoke
   :f :read})

(defn- cluster-test
  [test extra-sessions]
  (-> test
      (assoc :nodes (:cluster-nodes test))
      (update :sessions merge extra-sessions)))

(defn- extra-cluster-sessions
  [test]
  (let [connected (set (keys (:sessions test)))]
    (->> (:cluster-nodes test)
         (remove connected)
         (map (fn [node]
                [node (control/session node)]))
         (into {}))))

(defn- app-db-grudge
  [test]
  (nemesis/complete-grudge [(:app-nodes test)
                            [(:db-node test)]]))

(defrecord AppDbPartitionNemesis [inner extra-sessions]
  nemesis/Reflection
  (fs [_]
    #{:start-partition :stop-partition})

  nemesis/Nemesis
  (setup! [_ test]
    (let [extra-sessions (extra-cluster-sessions test)]
      (->AppDbPartitionNemesis
        (nemesis/setup! (nemesis/partitioner)
                        (cluster-test test extra-sessions))
        extra-sessions)))

  (invoke! [_ test op]
    (let [fault-test (cluster-test test extra-sessions)
          response (case (:f op)
                     :start-partition
                     (nemesis/invoke! inner
                                      fault-test
                                      (assoc op :f :start
                                                :value (app-db-grudge test)))

                     :stop-partition
                     (nemesis/invoke! inner
                                      fault-test
                                      (assoc op :f :stop
                                                :value nil))

                     (assoc op :type :info :error (str "unknown nemesis op " (:f op))))]
      (assoc response :f (:f op))))

  (teardown! [_ test]
    (try
      (when inner
        (nemesis/teardown! inner (cluster-test test extra-sessions)))
      (finally
        (doseq [[_ session] extra-sessions]
          (control/disconnect session))))))

(defn- stop-postgres!
  [opts node]
  (remote-bash!
    opts
    node
    (str/join
      "\n"
      ["set -euo pipefail"
       "systemctl stop postgresql"
       "for _ in {1..30}; do"
       "  if ! pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then"
       "    exit 0"
       "  fi"
       "  sleep 1"
       "done"
       "echo 'Postgres still accepting connections after stop' >&2"
       "exit 1"])))

(defn- start-postgres!
  [opts node]
  (remote-bash!
    opts
    node
    (str/join
      "\n"
      ["set -euo pipefail"
       "systemctl start postgresql"
       "for _ in {1..30}; do"
       "  if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then"
       "    exit 0"
       "  fi"
       "  sleep 1"
       "done"
       "echo 'Postgres did not become ready after start' >&2"
       "exit 1"])))

(defrecord DbRestartNemesis [opts]
  nemesis/Reflection
  (fs [_]
    #{:stop-db :start-db})

  nemesis/Nemesis
  (setup! [this test]
    (start-postgres! opts (:db-node test))
    this)

  (invoke! [_ test op]
    (let [db-node (:db-node test)]
      (case (:f op)
        :stop-db
        (do
          (stop-postgres! opts db-node)
          (assoc op :type :info :value {db-node :stopped}))

        :start-db
        (do
          (start-postgres! opts db-node)
          (assoc op :type :info :value {db-node :started}))

        (assoc op :type :info :error (str "unknown nemesis op " (:f op))))))

  (teardown! [_ test]
    (start-postgres! opts (:db-node test))))

(def ^:private noop-nemesis-package
  {:generator nil
   :final-generator nil
   :nemesis nemesis/noop
   :perf #{}})

(defn- with-nemesis-warmup
  [generator]
  (gen/phases
    (gen/sleep default-nemesis-warmup-s)
    generator))

(defn- periodic-nemesis-generator
  [opts start-op stop-op]
  (with-nemesis-warmup
    (->> (gen/flip-flop start-op (gen/repeat stop-op))
         (gen/stagger (:nemesis-interval-s opts)))))

(defn- fault-package
  [opts]
  (when (and (not= "none" (:nemesis opts))
             (not (docker-topology? opts)))
    (throw (ex-info "Fault injection requires --topology docker"
                    {:nemesis (:nemesis opts)})))
  (case (:nemesis opts)
    "partition-app-db"
    {:generator (periodic-nemesis-generator opts
                                            {:type :info :f :start-partition :value nil}
                                            {:type :info :f :stop-partition :value nil})
     :final-generator {:type :info :f :stop-partition :value nil}
     :nemesis (->AppDbPartitionNemesis nil {})
     :perf #{{:name "partition"
              :start #{:start-partition}
              :stop #{:stop-partition}
              :color "#E9DCA0"}}}

    "restart-db"
    {:generator (periodic-nemesis-generator opts
                                            {:type :info :f :stop-db :value nil}
                                            {:type :info :f :start-db :value nil})
     :final-generator {:type :info :f :start-db :value nil}
     :nemesis (->DbRestartNemesis opts)
     :perf #{{:name "db-restart"
              :start #{:stop-db}
              :stop #{:start-db}
              :color "#E9A4A0"}}}

    noop-nemesis-package))

(defn- final-read-generator
  []
  (gen/clients (gen/each-thread (gen/once read-op))))

(defn- workload-generator
  [opts package]
  (let [client-generator (->> (gen/mix [diff-transfer read-op])
                              (gen/stagger (/ (double (:stagger-ms opts)) 1000.0)))
        run-generator (->> (if-let [nemesis-generator (:generator package)]
                             (gen/nemesis nemesis-generator client-generator)
                             (gen/clients client-generator))
                           (gen/time-limit (:time-limit opts)))]
    (gen/phases
      run-generator
      (when-let [final-generator (:final-generator package)]
        (gen/nemesis final-generator))
      (final-read-generator))))

(defrecord SendapayClient [opts target-node]
  client/Client
  (open! [_ _test node]
    (->SendapayClient opts (when (docker-topology? opts) node)))
  (setup! [this _test]
    this)
  (teardown! [_this _test])
  (close! [_this _test])
  (invoke! [_this _test op]
    (case (:f op)
      :read
      (let [response (helper-call-on! opts target-node "read"
                                      "--state-file" (:state-file opts))]
        (if (= "ok" (get response "result"))
          (assoc op :type :ok :value (get response "balances"))
          (assoc op :type :info :error (or (get response "error")
                                           "read failed"))))

      :transfer
      (let [{:keys [from to amount]} (:value op)
            response (helper-call-on! opts target-node "transfer"
                                      "--state-file" (:state-file opts)
                                      "--from-account" from
                                      "--to-account" to
                                      "--amount-cents" (str amount))]
        (case (get response "result")
          "ok" (assoc op :type :ok)
          "fail" (assoc op :type :fail :error (or (get response "error")
                                                  "transfer failed"))
          (assoc op :type :info :error (or (get response "error")
                                           "transfer crashed"))))

      (assoc op :type :info :error (str "unknown function " (:f op))))))

(defn- initialize-state!
  [opts]
  (when (docker-topology? opts)
    (ensure-docker-topology! opts)
    (reset-docker-logs! opts))
  (apply helper-call-on!
         opts
         (when (docker-topology? opts)
           (:primary-app-node opts))
         ["init"
          "--sendapay-root" (if (docker-topology? opts)
                              (:remote-sendapay-root opts)
                              (:sendapay-root opts))
          "--state-file" (:state-file opts)
          "--db-url" (:db-url opts)
          "--account-count" (str (:account-count opts))
          "--initial-balance-cents" (str (:initial-balance-cents opts))
          "--user-start-id" (str (:user-start-id opts))
          "--phone-prefix" (:phone-prefix opts)
          "--pool-size" (str (:pool-size opts))
          "--max-overflow" (str (:max-overflow opts))
          (when (:append-only opts) "--append-only")]))

(defn- copy-local-file-artifact!
  [test checker-opts source-path & artifact-parts]
  (let [source (io/file source-path)
        destination (apply artifact-path! test checker-opts artifact-parts)]
    (if (.exists source)
      (with-open [in (io/input-stream source)
                  out (io/output-stream destination)]
        (io/copy in out))
      (spit destination (str "missing local artifact source: " source-path "\n")))
    (.getPath destination)))

(defn- capture-remote-file-artifact!
  [opts test checker-opts node source-path & artifact-parts]
  (let [destination (apply artifact-path! test checker-opts artifact-parts)]
    (try
      (let [{:keys [out]} (remote-bash!
                            opts
                            node
                            (str/join
                              "\n"
                              ["set -euo pipefail"
                               (str "cat " (shell-quote source-path))]))]
        (spit destination out)
        {:status :ok
         :node node
         :source source-path
         :artifact (.getPath destination)})
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [stderr exit]} (ex-data e)]
          (spit destination
                (str "failed to capture remote artifact\n"
                     "node: " node "\n"
                     "source: " source-path "\n"
                     "exit: " exit "\n"
                     "stderr:\n" stderr "\n"))
          {:status :error
           :node node
           :source source-path
           :artifact (.getPath destination)
           :error (.getMessage e)})))))

(defn- capture-remote-command-artifact!
  [opts test checker-opts node script source-label & artifact-parts]
  (let [destination (apply artifact-path! test checker-opts artifact-parts)]
    (try
      (let [{:keys [out]} (remote-bash! opts node script)]
        (spit destination out)
        {:status :ok
         :node node
         :source source-label
         :artifact (.getPath destination)})
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [stderr exit stdout]} (ex-data e)]
          (spit destination
                (str "failed to capture remote command artifact\n"
                     "node: " node "\n"
                     "source: " source-label "\n"
                     "exit: " exit "\n"
                     "stdout:\n" stdout "\n"
                     "stderr:\n" stderr "\n"))
          {:status :error
           :node node
           :source source-label
           :artifact (.getPath destination)
           :error (.getMessage e)})))))

(defrecord ArtifactCollectionChecker [opts]
  checker/Checker
  (check [_ test _history checker-opts]
    (let [helper-artifacts (for [node (:app-nodes opts)
                                 :let [source (helper-log-path opts node)
                                       artifact-path (copy-local-file-artifact!
                                                       test
                                                       checker-opts
                                                       source
                                                       "app"
                                                       node
                                                       "helper.log")]]
                             {:status :ok
                              :node node
                              :source source
                              :artifact artifact-path})
          state-artifact {:status :ok
                          :source (:state-file opts)
                          :artifact (copy-local-file-artifact!
                                      test
                                      checker-opts
                                      (:state-file opts)
                                      "state"
                                      "sendapay-bank-state.json")}
          db-artifact (capture-remote-file-artifact!
                        opts
                        test
                        checker-opts
                        (:db-node opts)
                        default-postgres-log-path
                        "db"
                        (:db-node opts)
                        "postgresql.log")
          db-journal-artifact (capture-remote-command-artifact!
                                opts
                                test
                                checker-opts
                                (:db-node opts)
                                (str/join
                                  "\n"
                                  ["set -euo pipefail"
                                   "journalctl -u postgresql --no-pager -n 500"])
                                "journalctl -u postgresql --no-pager -n 500"
                                "db"
                                (:db-node opts)
                                "postgresql-journal.log")]
      {:valid? true
       :artifacts (vec (concat helper-artifacts [state-artifact db-artifact db-journal-artifact]))})))

(defn- sendapay-test
  [opts]
  (let [state (initialize-state! opts)
        package (fault-package opts)
        tracked-accounts (vec (get state "tracked_accounts"))
        transfer-accounts (vec (get state "transfer_accounts"))
        total-amount (long (get state "total_amount_cents"))
        docker? (docker-topology? opts)
        test-name (if (:append-only opts)
                    "sendapay-bank-append-only"
                    "sendapay-bank-classic")]
    (merge tests/noop-test
           {:pure-generators true
            :name test-name
            :nodes (if docker?
                     (:app-nodes opts)
                     ["local"])
            :ssh (if docker?
                   (cond-> {:username (:ssh-user opts)
                            :password (:ssh-password opts)
                            :strict-host-key-checking false}
                     (:ssh-private-key-path opts)
                     (assoc :private-key-path (:ssh-private-key-path opts)))
                   {:dummy? true
                    :strict-host-key-checking false})
            :concurrency (:concurrency opts)
            :accounts tracked-accounts
            :transfer-accounts transfer-accounts
            :total-amount total-amount
            :max-transfer (:max-transfer-cents opts)
            :generator (workload-generator opts package)
            :checker (checker/compose {:bank (bank/checker {:negative-balances? true})
                                       :plot (bank/plotter)
                                       :artifacts (->ArtifactCollectionChecker opts)})
            :client (->SendapayClient opts nil)
            :nemesis (:nemesis package)
            :plot {:nemeses (:perf package)}
            :app-nodes (:app-nodes opts)
            :primary-app-node (:primary-app-node opts)
            :cluster-nodes (:cluster-nodes opts)
            :db-node (:db-node opts)
            :sendapay-root (:sendapay-root opts)
            :remote-sendapay-root (:remote-sendapay-root opts)
            :state-file (:state-file opts)
            :db-url (:db-url opts)
            :initial-state state})))

(defn- usage
  [summary]
  (str "Usage: lein run -- [options]\n\nOptions:\n"
       summary))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      (do
        (println (usage summary))
        (System/exit 0))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [error errors]
            (println error))
          (println)
          (println (usage summary)))
        (System/exit 2))

      :else
      (let [opts (normalized-options options)
            result (jepsen/run! (sendapay-test opts))
            valid? (boolean (get-in result [:results :valid?]))
            bank-results (get-in result [:results :bank])]
        (println
          (json/generate-string
            {"valid" valid?
             "bank" bank-results}
            {:pretty true}))
        (when-not valid?
          (System/exit 1))))))
