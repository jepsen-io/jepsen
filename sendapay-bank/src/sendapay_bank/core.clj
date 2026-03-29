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

(def ^:private jepsen-control-banner-lines
  #{"Welcome to Jepsen on Docker"
    "==========================="
    "This container runs the Jepsen tests in sub-containers."
    "You are currently in the base dir of the git repo for Jepsen."
    "If you modify the core jepsen library make sure you \"lein install\" it so other tests can access."
    "To run a test:"
    "   cd etcd && lein run test --concurrency 10"})

(def ^:private helper-summary-line-limit
  120)

(def ^:private helper-tail-line-limit
  40)

(defn- nemesis-snapshot-root
  [opts]
  (str (:remote-helper-root opts) "/nemesis-snapshots"))

(defn- normalize-artifact-label
  [s]
  (let [normalized (-> (str s)
                       str/lower-case
                       (str/replace #"[^a-z0-9]+" "-")
                       (str/replace #"(^-|-$)" ""))]
    (if (str/blank? normalized)
      "snapshot"
      normalized)))

(defn- reset-local-nemesis-snapshots!
  [opts]
  (let [root (io/file (nemesis-snapshot-root opts))]
    (.mkdirs root)
    (doseq [child (reverse (sort-by #(count (.getPath %))
                                    (rest (file-seq root))))]
      (when-not (.delete child)
        (throw (ex-info "failed to clear nemesis snapshot path"
                        {:path (.getPath child)}))))))

(defn- local-nemesis-snapshot-files
  [opts]
  (let [root (io/file (nemesis-snapshot-root opts))]
    (if (.exists root)
      (->> (rest (file-seq root))
           (filter #(.isFile %))
           (sort-by #(.getName %))
           (map #(.getCanonicalPath %)))
      [])))

(defn- nemesis-snapshot-relative-parts
  [opts source-path]
  (let [root-path (.toPath (io/file (nemesis-snapshot-root opts)))
        source (.toPath (io/file source-path))
        relative (.relativize root-path source)]
    (vec (map str (iterator-seq (.iterator relative))))))

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

(def ^:private ssh-known-host-warning-pattern
  #"(?m)^Warning: Permanently added '.*' \([^)]+\) to the list of known hosts\.\r?\n?")

(defn- strip-jepsen-control-banner
  [text]
  (let [lines (str/split-lines (or text ""))]
    (str/join
      "\n"
      (drop-while #(or (contains? jepsen-control-banner-lines %)
                       (str/blank? %))
                  lines))))

(defn- strip-ssh-known-host-warnings
  [text]
  (str/replace (or text "") ssh-known-host-warning-pattern ""))

(defn- strip-terminal-noise
  [text]
  (-> text
      strip-jepsen-control-banner
      strip-ssh-known-host-warnings))

(defn- sanitized-tabular-output
  [text]
  (let [cleaned (str/trim (strip-terminal-noise text))]
    (when-not (str/blank? cleaned)
      (str cleaned "\n"))))

(defn- tsv-safe
  [value]
  (-> (str (or value ""))
      (str/replace #"\r\n?|\n" " | ")
      (str/replace #"\t" " ")
      (str/trim)))

(defn- parse-long-safe
  [value default]
  (try
    (Long/parseLong (str/trim (str value)))
    (catch Exception _
      default)))

(defn- ssh-target
  [opts node]
  (str (:ssh-user opts) "@" node))

(defn- remote-bash!
  [opts node script]
  (let [ssh-script (str "ssh -o LogLevel=ERROR "
                        "-o StrictHostKeyChecking=no "
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
    (reset-local-nemesis-snapshots! opts)
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

(declare capture-nemesis-postgres-snapshots!)
(declare capture-nemesis-app-snapshots!)

(defrecord AppDbPartitionNemesis [opts inner extra-sessions]
  nemesis/Reflection
  (fs [_]
    #{:start-partition :stop-partition})

  nemesis/Nemesis
  (setup! [_ test]
    (let [extra-sessions (extra-cluster-sessions test)]
      (->AppDbPartitionNemesis
        opts
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
      (when (#{:start-partition :stop-partition} (:f op))
        (capture-nemesis-postgres-snapshots!
          opts
          (:db-node test)
          (str "after-" (name (:f op))))
        (capture-nemesis-app-snapshots!
          opts
          (str "after-" (name (:f op)))))
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
          (capture-nemesis-postgres-snapshots! opts db-node "after-stop-db")
          (capture-nemesis-app-snapshots! opts "after-stop-db")
          (assoc op :type :info :value {db-node :stopped}))

        :start-db
        (do
          (start-postgres! opts db-node)
          (capture-nemesis-postgres-snapshots! opts db-node "after-start-db")
          (capture-nemesis-app-snapshots! opts "after-start-db")
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
     :nemesis (->AppDbPartitionNemesis opts nil {})
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

(defn- postgres-query-script
  [sql]
  (str/join
    "\n"
    ["set -euo pipefail"
     "export PGPASSWORD=sendapay"
     "psql --no-psqlrc -X -A -F $'\\t' -P pager=off -h 127.0.0.1 -p 5432 -U sendapay -d sendapay_jepsen_bank -v ON_ERROR_STOP=1 <<'SQL'"
     sql
     "SQL"]))

(defn- render-snapshot-error-tsv
  [capture-label node source-label exit stdout stderr]
  (let [clean-stdout (tsv-safe (sanitized-tabular-output stdout))
        clean-stderr (tsv-safe (sanitized-tabular-output stderr))
        message (or (not-empty clean-stderr)
                    (not-empty clean-stdout)
                    "snapshot capture failed")]
    (str "status\tcapture_label\tnode\tsource\texit\tmessage\n"
         (str/join "\t" ["error"
                         (tsv-safe capture-label)
                         (tsv-safe node)
                         (tsv-safe source-label)
                         (tsv-safe exit)
                         message])
         "\n")))

(defn- capture-postgres-query-to-path!
  [opts node destination capture-label source-label sql]
  (ensure-parent! (.getPath destination))
  (try
    (let [{:keys [out]} (remote-bash! opts node (postgres-query-script sql))]
      (spit destination (or (sanitized-tabular-output out) ""))
      {:status :ok
       :node node
       :source source-label
       :artifact (.getPath destination)})
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [stderr exit stdout]} (ex-data e)]
        (spit destination
              (render-snapshot-error-tsv capture-label
                                         node
                                         source-label
                                         exit
                                         stdout
                                         stderr))
        {:status :error
         :node node
         :source source-label
         :artifact (.getPath destination)
         :error (.getMessage e)}))))

(defn- capture-remote-postgres-query-artifact!
  [opts test checker-opts node source-label sql & artifact-parts]
  (let [destination (apply artifact-path! test checker-opts artifact-parts)]
    (capture-postgres-query-to-path!
      opts
      node
      destination
      "end-of-run"
      source-label
      sql)))

(def ^:private pg-stat-activity-query
  (str/join
    "\n"
    ["SELECT pid,"
     "       usename,"
     "       application_name,"
     "       backend_type,"
     "       client_addr,"
     "       state,"
     "       wait_event_type,"
     "       wait_event,"
     "       xact_start,"
     "       query_start,"
     "       state_change,"
     "       left(replace(replace(replace(coalesce(query, ''), E'\\n', ' '), E'\\r', ' '), E'\\t', ' '), 300) AS query"
     "FROM pg_stat_activity"
     "WHERE datname = current_database() OR pid = pg_backend_pid()"
     "ORDER BY state, xact_start NULLS LAST, query_start NULLS LAST, pid;"]))

(def ^:private pg-locks-query
  (str/join
    "\n"
    ["SELECT l.pid,"
     "       coalesce(a.usename, '') AS usename,"
     "       coalesce(a.application_name, '') AS application_name,"
     "       l.locktype,"
     "       l.mode,"
     "       l.granted,"
     "       coalesce(l.relation::regclass::text, '') AS relation,"
     "       coalesce(l.page::text, '') AS page,"
     "       coalesce(l.tuple::text, '') AS tuple,"
     "       coalesce(l.virtualxid, '') AS virtualxid,"
     "       coalesce(l.transactionid::text, '') AS transactionid,"
     "       coalesce(a.state, '') AS state,"
     "       coalesce(a.wait_event_type, '') AS wait_event_type,"
     "       coalesce(a.wait_event, '') AS wait_event"
     "FROM pg_locks l"
     "LEFT JOIN pg_stat_activity a ON a.pid = l.pid"
     "LEFT JOIN pg_database d ON d.oid = l.database"
     "WHERE d.datname = current_database() OR d.datname IS NULL"
     "ORDER BY l.granted, l.pid, l.locktype, l.mode;"]))

(defn- read-last-lines
  [path limit]
  (let [source (io/file path)]
    (if (.exists source)
      (->> (str/split-lines (slurp source))
           (take-last limit)
           vec)
      [])))

(def ^:private helper-python-process-filter
  "$5 ~ /^python([0-9.]*)?$/ && index($0, \"sendapay_bank_ops.py\") > 0")

(defn- helper-process-count-script
  []
  (str/join
    "\n"
    ["set -euo pipefail"
     (str "count=$(ps -eo pid=,ppid=,stat=,etime=,comm=,args= "
          "| awk '" helper-python-process-filter " { count++ } END { print count + 0 }')")
     "printf '%s\n' \"$count\""]))

(defn- helper-process-count
  [opts node]
  (try
    (let [{:keys [out]} (remote-bash! opts
                                      node
                                      (helper-process-count-script))]
      (parse-long-safe (str/trim (or (strip-terminal-noise out) "")) 0))
    (catch clojure.lang.ExceptionInfo _
      -1)))

(def ^:private helper-summary-columns
  [:status
   :capture_label
   :node
   :recent_line_count
   :command_count
   :json_ok_count
   :json_fail_count
   :json_other_count
   :http_status_200_count
   :http_status_non_200_count
   :rq_queue_unavailable_count
   :operational_error_count
   :connection_error_count
   :security_warning_count
   :transfer_confirmed_count
   :transfer_intent_created_count
   :helper_process_count])

(def ^:private db-socket-summary-columns
  [:status
   :capture_label
   :node
   :total_connection_count
   :established_count
   :syn_sent_count
   :syn_recv_count
   :time_wait_count
   :close_wait_count
   :fin_wait_1_count
   :fin_wait_2_count
   :last_ack_count
   :closing_count
   :listen_count
   :other_count])

(def ^:private db-socket-state-key
  {"ESTAB" :established_count
   "SYN-SENT" :syn_sent_count
   "SYN-RECV" :syn_recv_count
   "TIME-WAIT" :time_wait_count
   "CLOSE-WAIT" :close_wait_count
   "FIN-WAIT-1" :fin_wait_1_count
   "FIN-WAIT-2" :fin_wait_2_count
   "LAST-ACK" :last_ack_count
   "CLOSING" :closing_count
   "LISTEN" :listen_count})

(defn- helper-summary-metrics
  [capture-label node lines helper-process-count]
  {:status (if (seq lines) "ok" "missing")
   :capture_label capture-label
   :node node
   :recent_line_count (count lines)
   :command_count (count (filter #(str/starts-with? % "=== ") lines))
   :json_ok_count (count (filter #(re-find #"\"result\"\s*:\s*\"ok\"" %) lines))
   :json_fail_count (count (filter #(re-find #"\"result\"\s*:\s*\"fail\"" %) lines))
   :json_other_count (count (filter #(re-find #"\"result\"\s*:\s*\"(?!ok|fail)[^\"]+\"" %) lines))
   :http_status_200_count (count (filter #(re-find #"\"status\"\s*:\s*200\b" %) lines))
   :http_status_non_200_count (count (filter #(re-find #"\"status\"\s*:\s*(?!200\b)\d+" %) lines))
   :rq_queue_unavailable_count (count (filter #(re-find #"\"reason\"\s*:\s*\"rq_queue_unavailable\"" %) lines))
   :operational_error_count (count (filter #(re-find #"OperationalError" %) lines))
   :connection_error_count (count (filter #(re-find #"connection (?:failed|timeout expired)|database system is shutting down" %) lines))
   :security_warning_count (count (filter #(str/includes? % "SECURITY_MODE=MOCKS_ENABLED") lines))
   :transfer_confirmed_count (count (filter #(re-find #"\"event\"\s*:\s*\"transfer_confirmed\"" %) lines))
   :transfer_intent_created_count (count (filter #(re-find #"\"event\"\s*:\s*\"transfer_intent_created\"" %) lines))
   :helper_process_count helper-process-count})

(defn- render-helper-summary-tsv
  [capture-label node lines helper-process-count]
  (let [metrics (helper-summary-metrics capture-label node lines helper-process-count)]
    (str (str/join "\t" (map name helper-summary-columns))
         "\n"
         (str/join "\t" (map #(tsv-safe (get metrics %)) helper-summary-columns))
         "\n")))

(defn- app-db-sockets-script
  []
  (str/join
    "\n"
    ["set -euo pipefail"
     "ss -tanH | grep ':5432' || true"]))

(defn- app-processes-script
  []
  (str/join
    "\n"
    ["set -euo pipefail"
     (str "ps -eo pid=,ppid=,stat=,etime=,comm=,args= "
          "| awk '" helper-python-process-filter " { print }'")]))

(defn- remote-command-output
  [opts node script]
  (try
    (let [{:keys [out]} (remote-bash! opts node script)]
      {:status :ok
       :out (or (sanitized-tabular-output out) "")})
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [stderr exit stdout]} (ex-data e)]
        {:status :error
         :exit exit
         :stdout (or (sanitized-tabular-output stdout) "")
         :stderr (or (sanitized-tabular-output stderr) "")
         :error (.getMessage e)}))))

(defn- render-remote-command-error-log
  [capture-label node source-label exit stdout stderr]
  (str "capture_label: " capture-label "\n"
       "node: " node "\n"
       "source: " source-label "\n"
       "exit: " exit "\n"
       (when-not (str/blank? stdout)
         (str "stdout:\n" stdout
              (when-not (str/ends-with? stdout "\n")
                "\n")))
       (when-not (str/blank? stderr)
         (str "stderr:\n" stderr
              (when-not (str/ends-with? stderr "\n")
                "\n")))))

(defn- db-socket-summary-metrics
  [capture-label node lines]
  (let [lines (vec (remove str/blank? lines))
        metrics (merge {:status "ok"
                        :capture_label capture-label
                        :node node
                        :total_connection_count (count lines)
                        :other_count 0}
                       (into {}
                             (map (fn [column] [column 0])
                                  (distinct (vals db-socket-state-key)))))]
    (reduce (fn [acc line]
              (let [state (some-> line
                                  str/trim
                                  (str/split #"\s+" 2)
                                  first
                                  str/upper-case)
                    key (get db-socket-state-key state :other_count)]
                (update acc key (fnil inc 0))))
            metrics
            lines)))

(defn- render-db-socket-summary-tsv
  [capture-label node lines]
  (let [metrics (db-socket-summary-metrics capture-label node lines)]
    (str (str/join "\t" (map name db-socket-summary-columns))
         "\n"
         (str/join "\t" (map #(tsv-safe (get metrics %)) db-socket-summary-columns))
         "\n")))

(defn- capture-app-db-socket-snapshot!
  [opts node capture-label summary-path raw-path]
  (let [{:keys [status out exit stdout stderr]} (remote-command-output opts
                                                                       node
                                                                       (app-db-sockets-script))]
    (ensure-parent! (.getPath summary-path))
    (ensure-parent! (.getPath raw-path))
    (if (= :ok status)
      (let [lines (->> (str/split-lines out)
                       (remove str/blank?)
                       vec)]
        (spit summary-path
              (render-db-socket-summary-tsv capture-label node lines))
        (spit raw-path
              (if (seq lines)
                (str (str/join "\n" lines) "\n")
                "")))
      (do
        (spit summary-path
              (render-snapshot-error-tsv capture-label
                                         node
                                         "app_db_sockets"
                                         exit
                                         stdout
                                         stderr))
        (spit raw-path
              (render-remote-command-error-log capture-label
                                               node
                                               "app_db_sockets"
                                               exit
                                               stdout
                                               stderr))))))

(defn- capture-app-process-snapshot!
  [opts node capture-label path]
  (let [{:keys [status out exit stdout stderr]} (remote-command-output opts
                                                                       node
                                                                       (app-processes-script))]
    (ensure-parent! (.getPath path))
    (if (= :ok status)
      (spit path
            (if (str/blank? out)
              "no running sendapay helper python processes\n"
              out))
      (spit path
            (render-remote-command-error-log capture-label
                                             node
                                             "app_processes"
                                             exit
                                             stdout
                                             stderr)))))

(defn- capture-nemesis-app-snapshots!
  [opts label]
  (let [prefix (str (System/currentTimeMillis)
                    "-"
                    (normalize-artifact-label label))]
    (doseq [node (:app-nodes opts)]
      (let [snapshot-root (io/file (nemesis-snapshot-root opts) "app" node)
            source-path (helper-log-path opts node)
            summary-path (io/file snapshot-root (str prefix "-helper-summary.tsv"))
            tail-path (io/file snapshot-root (str prefix "-helper-tail.log"))
            db-socket-summary-path (io/file snapshot-root (str prefix "-db-sockets.tsv"))
            db-socket-log-path (io/file snapshot-root (str prefix "-db-sockets.log"))
            processes-path (io/file snapshot-root (str prefix "-processes.log"))
            lines (read-last-lines source-path helper-summary-line-limit)
            tail-lines (take-last helper-tail-line-limit lines)
            process-count (helper-process-count opts node)]
        (.mkdirs snapshot-root)
        (spit summary-path
              (render-helper-summary-tsv label node lines process-count))
        (spit tail-path
              (if (seq tail-lines)
                (str (str/join "\n" tail-lines) "\n")
                (str "missing helper log: " source-path "\n")))
        (capture-app-db-socket-snapshot! opts node label db-socket-summary-path db-socket-log-path)
        (capture-app-process-snapshot! opts node label processes-path)))))

(defn- capture-nemesis-postgres-snapshots!
  [opts db-node label]
  (let [snapshot-root (io/file (nemesis-snapshot-root opts) "db" db-node)
        prefix (str (System/currentTimeMillis)
                    "-"
                    (normalize-artifact-label label))
        capture-query! (fn [suffix source-label sql]
                         (let [destination (io/file snapshot-root
                                                    (str prefix "-" suffix))]
                           (capture-postgres-query-to-path!
                             opts
                             db-node
                             destination
                             label
                             source-label
                             sql)))]
    (.mkdirs snapshot-root)
    (capture-query! "pg-stat-activity.tsv"
                    "pg_stat_activity"
                    pg-stat-activity-query)
    (capture-query! "pg-locks.tsv"
                    "pg_locks"
                    pg-locks-query)))

(def ^:private snapshot-error-header
  ["status" "capture_label" "node" "source" "exit" "message"])

(def ^:private snapshot-kind-suffixes
  [[:pg-stat-activity "-pg-stat-activity.tsv"]
   [:pg-locks "-pg-locks.tsv"]
   [:db-sockets "-db-sockets.tsv"]
   [:db-sockets-log "-db-sockets.log"]
   [:helper-summary "-helper-summary.tsv"]
   [:helper-tail "-helper-tail.log"]
   [:processes "-processes.log"]])

(def ^:private helper-summary-numeric-keys
  #{:recent_line_count
    :command_count
    :json_ok_count
    :json_fail_count
    :json_other_count
    :http_status_200_count
    :http_status_non_200_count
    :rq_queue_unavailable_count
    :operational_error_count
    :connection_error_count
    :security_warning_count
    :transfer_confirmed_count
    :transfer_intent_created_count
    :helper_process_count})

(def ^:private db-socket-summary-numeric-keys
  #{:total_connection_count
    :established_count
    :syn_sent_count
    :syn_recv_count
    :time_wait_count
    :close_wait_count
    :fin_wait_1_count
    :fin_wait_2_count
    :last_ack_count
    :closing_count
    :listen_count
    :other_count})

(defn- snapshot-kind
  [file-name]
  (some (fn [[kind suffix]]
          (when (str/ends-with? file-name suffix)
            kind))
        snapshot-kind-suffixes))

(defn- snapshot-suffix
  [kind]
  (some (fn [[candidate suffix]]
          (when (= kind candidate)
            suffix))
        snapshot-kind-suffixes))

(defn- parse-tsv-file
  [path]
  (let [lines (->> (str/split-lines (slurp path))
                   (remove str/blank?)
                   vec)]
    (if (seq lines)
      (let [header (vec (str/split (first lines) #"\t" -1))]
        {:header header
         :rows (mapv #(zipmap header (str/split % #"\t" -1)) (rest lines))})
      {:header []
       :rows []})))

(defn- snapshot-file-metadata
  [opts source-path]
  (let [[category node file-name] (nemesis-snapshot-relative-parts opts source-path)
        kind (snapshot-kind file-name)
        suffix (snapshot-suffix kind)
        base (if suffix
               (subs file-name 0 (- (count file-name) (count suffix)))
               file-name)
        [_ timestamp-ms capture-label] (re-matches #"(\d+)-(.*)" base)]
    {:source source-path
     :category category
     :node node
     :file-name file-name
     :kind kind
     :capture-id base
     :capture-label (or capture-label base)
     :timestamp-ms (parse-long-safe timestamp-ms -1)}))

(defn- parse-error-snapshot
  [metadata path]
  (let [{:keys [rows]} (parse-tsv-file path)
        row (first rows)]
    (merge metadata
           {:capture-status :error
            :exit (parse-long-safe (get row "exit") -1)
            :message (get row "message")})))

(defn- parse-summary-snapshot
  [metadata path numeric-keys]
  (let [{:keys [header rows]} (parse-tsv-file path)]
    (if (= header snapshot-error-header)
      (parse-error-snapshot metadata path)
      (let [row (or (first rows) {})
            parsed-row (reduce-kv
                         (fn [acc raw-key raw-value]
                           (let [key (keyword raw-key)]
                             (assoc acc
                                    key
                                    (if (numeric-keys key)
                                      (parse-long-safe raw-value 0)
                                      raw-value))))
                         {}
                         row)]
        (merge metadata
               {:capture-status (keyword (or (:status parsed-row) "missing"))}
               parsed-row)))))

(defn- parse-pg-stat-activity-snapshot
  [metadata path]
  (let [{:keys [header rows]} (parse-tsv-file path)]
    (if (= header snapshot-error-header)
      (parse-error-snapshot metadata path)
      (let [blocked-rows (filter #(= "Lock" (get % "wait_event_type")) rows)
            waiting-rows (filter #(let [wait-type (get % "wait_event_type")]
                                    (and (not (str/blank? wait-type))
                                         (not (#{"Client" "Activity"} wait-type))))
                                 rows)]
        (merge metadata
               {:capture-status :ok
                :session-count (count rows)
                :active-session-count (count (filter #(= "active" (get % "state")) rows))
                :waiting-session-count (count waiting-rows)
                :blocked-session-count (count blocked-rows)})))))

(defn- parse-pg-locks-snapshot
  [metadata path]
  (let [{:keys [header rows]} (parse-tsv-file path)]
    (if (= header snapshot-error-header)
      (parse-error-snapshot metadata path)
      (let [ungranted (filter #(not (#{"t" "true" "1"} (str/lower-case (get % "granted" "")))) rows)]
        (merge metadata
               {:capture-status :ok
                :total-lock-count (count rows)
                :granted-lock-count (- (count rows) (count ungranted))
                :ungranted-lock-count (count ungranted)
                :blocked-session-count (count (set (map #(get % "pid") ungranted)))})))))

(defn- parse-helper-summary-snapshot
  [metadata path]
  (parse-summary-snapshot metadata path helper-summary-numeric-keys))

(defn- parse-db-socket-snapshot
  [metadata path]
  (parse-summary-snapshot metadata path db-socket-summary-numeric-keys))

(defn- analyze-nemesis-snapshot
  [opts source-path]
  (let [metadata (snapshot-file-metadata opts source-path)]
    (case (:kind metadata)
      :pg-stat-activity (parse-pg-stat-activity-snapshot metadata source-path)
      :pg-locks (parse-pg-locks-snapshot metadata source-path)
      :db-sockets (parse-db-socket-snapshot metadata source-path)
      :helper-summary (parse-helper-summary-snapshot metadata source-path)
      :db-sockets-log nil
      :helper-tail nil
      :processes nil
      (assoc metadata :capture-status :unknown))))

(defn- summarize-pg-stat-activity
  [entries]
  {:count (count entries)
   :error-count (count (filter #(= :error (:capture-status %)) entries))
   :max-blocked-session-count (apply max 0 (map #(or (:blocked-session-count %) 0) entries))
   :max-waiting-session-count (apply max 0 (map #(or (:waiting-session-count %) 0) entries))
   :max-active-session-count (apply max 0 (map #(or (:active-session-count %) 0) entries))})

(defn- summarize-pg-locks
  [entries]
  {:count (count entries)
   :error-count (count (filter #(= :error (:capture-status %)) entries))
   :max-total-lock-count (apply max 0 (map #(or (:total-lock-count %) 0) entries))
   :max-ungranted-lock-count (apply max 0 (map #(or (:ungranted-lock-count %) 0) entries))
   :max-blocked-session-count (apply max 0 (map #(or (:blocked-session-count %) 0) entries))})

(defn- summarize-helper-snapshots
  [entries]
  {:count (count entries)
   :missing-count (count (filter #(= :missing (:capture-status %)) entries))
   :max-helper-process-count (apply max -1 (map #(or (:helper_process_count %)
                                                     (:helper-process-count %)
                                                     -1)
                                                entries))
   :max-rq-queue-unavailable-count (apply max 0 (map #(or (:rq_queue_unavailable_count %)
                                                          (:rq-queue-unavailable-count %)
                                                          0)
                                                     entries))
   :max-operational-error-count (apply max 0 (map #(or (:operational_error_count %)
                                                       (:operational-error-count %)
                                                       0)
                                                  entries))
   :max-connection-error-count (apply max 0 (map #(or (:connection_error_count %)
                                                      (:connection-error-count %)
                                                      0)
                                                 entries))
   :max-http-status-non-200-count (apply max 0 (map #(or (:http_status_non_200_count %)
                                                         (:http-status-non-200-count %)
                                                         0)
                                                    entries))})

(defn- summarize-db-socket-snapshots
  [entries]
  {:count (count entries)
   :error-count (count (filter #(= :error (:capture-status %)) entries))
   :max-total-connection-count (apply max 0 (map #(or (:total_connection_count %)
                                                      (:total-connection-count %)
                                                      0)
                                                 entries))
   :max-established-count (apply max 0 (map #(or (:established_count %)
                                                 (:established-count %)
                                                 0)
                                            entries))
   :max-syn-sent-count (apply max 0 (map #(or (:syn_sent_count %)
                                              (:syn-sent-count %)
                                              0)
                                         entries))
   :max-close-wait-count (apply max 0 (map #(or (:close_wait_count %)
                                                (:close-wait-count %)
                                                0)
                                           entries))
   :max-time-wait-count (apply max 0 (map #(or (:time_wait_count %)
                                               (:time-wait-count %)
                                               0)
                                          entries))
   :max-other-count (apply max 0 (map #(or (:other_count %)
                                           (:other-count %)
                                           0)
                                      entries))})

(defrecord NemesisSnapshotSummaryChecker [opts]
  checker/Checker
  (check [_ _test _history _checker-opts]
    (let [entries (->> (local-nemesis-snapshot-files opts)
                       (map #(analyze-nemesis-snapshot opts %))
                       (remove nil?)
                       (sort-by (juxt :timestamp-ms :category :node :file-name))
                       vec)
          db-entries (filter #(= "db" (:category %)) entries)
          app-entries (filter #(= "app" (:category %)) entries)]
      {:valid? true
       :snapshot-count (count entries)
       :db-snapshots {:pg-stat-activity (summarize-pg-stat-activity
                                          (filter #(= :pg-stat-activity (:kind %)) db-entries))
                      :pg-locks (summarize-pg-locks
                                  (filter #(= :pg-locks (:kind %)) db-entries))}
       :app-snapshots {:helper-summary (summarize-helper-snapshots
                                          (filter #(= :helper-summary (:kind %)) app-entries))
                       :db-sockets (summarize-db-socket-snapshots
                                     (filter #(= :db-sockets (:kind %)) app-entries))}
       :transitions (vec (map #(dissoc % :source) entries))})))

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
                                "postgresql-journal.log")
          db-stat-activity-artifact (capture-remote-postgres-query-artifact!
                                      opts
                                      test
                                      checker-opts
                                      (:db-node opts)
                                      "psql pg_stat_activity snapshot"
                                      pg-stat-activity-query
                                      "db"
                                      (:db-node opts)
                                      "pg-stat-activity.tsv")
          db-locks-artifact (capture-remote-postgres-query-artifact!
                              opts
                              test
                              checker-opts
                              (:db-node opts)
                              "psql pg_locks snapshot"
                              pg-locks-query
                              "db"
                              (:db-node opts)
                              "pg-locks.tsv")
          nemesis-snapshot-artifacts (for [source (local-nemesis-snapshot-files opts)
                                           :let [parts (nemesis-snapshot-relative-parts opts source)
                                                 artifact-parts (into [(first parts)
                                                                       (second parts)
                                                                       "nemesis"]
                                                                      (drop 2 parts))
                                                 artifact-path (apply copy-local-file-artifact!
                                                                      test
                                                                      checker-opts
                                                                      source
                                                                      artifact-parts)]]
                                       {:status :ok
                                        :node (second parts)
                                        :source source
                                        :artifact artifact-path})]
      {:valid? true
       :artifacts (vec (concat helper-artifacts
                               [state-artifact
                                db-artifact
                                db-journal-artifact
                                db-stat-activity-artifact
                                db-locks-artifact]
                               nemesis-snapshot-artifacts))})))

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
                                       :nemesis-summary (->NemesisSnapshotSummaryChecker opts)
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
