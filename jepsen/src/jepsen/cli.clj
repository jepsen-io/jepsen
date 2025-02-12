(ns jepsen.cli
  "Command line interface. Provides a default main method for common Jepsen
  functions (like the web interface), and utility functions for Jepsen tests to
  create their own test runners."
  (:gen-class)
  (:refer-clojure :exclude [run!])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [dom-top.core :refer [assert+]]
            [jepsen [core :as jepsen]
                    [store :as store]
                    [util :as util :refer [map-vals]]
                    [web :as web]]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (str/join ", " (sort (map name (if (map? coll) (keys coll) coll))))))

(defn repeated-opt
  "Helper for vector options where we want to replace the default vector
  (checking via identical?) if any options are passed, building a vector for
  multiple args. If parse-map is provided (a map of string cmdline options to
  parsed values), the special word \"all\" can be used to specify every value
  in the map."
  ([short-opt long-opt docstring default]
   [short-opt long-opt docstring
    :default default
    :assoc-fn (fn [m k v]
                (if (identical? (get m k) default)
                  (assoc m k [v])
                  (update m k conj v)))])
  ([short-opt long-opt docstring default parse-map]
   [short-opt long-opt docstring
    :default default
    :parse-fn (assoc parse-map "all" :all)
    :validate [identity (one-of parse-map)]
    :assoc-fn (fn [m k v]
                (if (= :all v)
                  (assoc m k (vals parse-map))
                  (if (identical? (get m k) default)
                    (assoc m k [v])
                    (update m k conj v))))]))

(defn merge-opt-specs
  "Takes two option specifications and merges them together. Where both offer
  the same option name, prefers the latter."
  [a b]
  (->> (merge (group-by second a)
              (group-by second b))
       vals
       (map first)))

(def help-opt
  ["-h" "--help" "Print out this message and exit"])

(def test-opt-spec
  "Command line options for testing."

  [help-opt

   (repeated-opt "-n" "--node HOSTNAME" "Node(s) to run test on. Flag may be submitted many times, with one node per flag." default-nodes)

   [nil "--nodes NODE_LIST" "Comma-separated list of node hostnames."
    :parse-fn #(str/split % #",\s*")]

   [nil "--nodes-file FILENAME" "File containing node hostnames, one per line."]

   [nil "--username USER" "Username for logins"
    :default "root"]

   [nil "--password PASS" "Password for sudo access"
    :default "root"]

   [nil "--strict-host-key-checking" "Whether to check host keys"
    :default false]

   [nil "--no-ssh" "If set, doesn't try to establish SSH connections to any nodes."
    :default false]

   [nil "--ssh-private-key FILE" "Path to an SSH identity file"]

   [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "1n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--leave-db-running" "Leave the database running at the end of the test., so you can inspect it."
    :default false]

   [nil "--logging-json" "Use JSON structured output in the Jepsen log."
    :default false]

   [nil "--test-count NUMBER"
    "How many times should we repeat a test?"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  60
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]])

(defn package-opt
  ([default]
   (package-opt "package-url" default))
  ([option-name default]
   [nil (str "--" option-name " URL")
    "URL for the DB package (a zip file or tarball) to install. May be either HTTP, HTTPS, or a local file present on each of the DB nodes. For instance, --tarball https://foo.com/db.tgz, or file:///tmp/db.zip"
    :default default
    :validate [(partial re-find #"^(file|https?)://.*\.(tar|tgz|zip)")
               "Must be a file://, http://, or https:// URL including .tar, .tgz, or .zip"]]))

(defn tarball-opt
  [default]
  (package-opt "tarball" default))

(defn test-usage
  []
  "Usage: lein run -- COMMAND [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  2     Some test had an :unknown validity
  254   Invalid arguments
  255   Internal Jepsen error

Options:\n")

;; Validation and option processing

(defn validate-tarball
  "Takes a parsed map and ensures a tarball is present."
  [parsed]
  (if (:tarball (:options parsed))
    parsed
    (update parsed :errors conj "No tarball URL provided")))

(defn parse-concurrency
  "Takes a parsed map. Parses :concurrency; if it is a string ending with n,
  e.g 3n, sets it to 3 * the number of :nodes. Otherwise, parses as a plain
  integer. With an optional keyword k, parses that key in the parsed map--by
  default, the key is :concurrency."
  ([parsed]
   (parse-concurrency parsed :concurrency))
  ([parsed k]
   (let [c (get (:options parsed) k)]
     (let [[match integer unit] (re-find #"(\d+)(n?)" c)]
       (when-not match
         (throw (IllegalArgumentException.
                  (str "--concurrency " c
                       " should be an integer optionally followed by n"))))
       (let [unit (if (= "n" unit)
                    (count (:nodes (:options parsed)))
                    1)]
         (assoc-in parsed [:options k]
                   (* unit (Long/parseLong integer))))))))

(defn parse-nodes
  "Takes a parsed map and merges all the various node specifications together.
  In particular:

  - If :nodes-file and :nodes are blank, and :node is the default node list,
    uses the default node list.
  - Otherwise, merges together :nodes-file, :nodes, and :node into a single
    list.

  The new parsed map will have a merged nodes list in :nodes, and lose
  :nodes-file and :node options."
  [parsed]
  (let [options       (:options parsed)
        node          (:node        options)
        nodes         (:nodes       options)
        nodes-file    (:nodes-file  options)

        ; If --node is still left at the default
        default-node? (identical? node default-nodes)
        ; ... and other options were given...
        override?     (or nodes nodes-file)
        ; Then drop the default node list
        node          (if (and default-node? override?) nil node)

        ; Read nodes-file nodes
        nodes-file    (when nodes-file
                        (str/split (slurp nodes-file) #"\s*\n\s*"))

        ; Construct full node list
        all-nodes     (->> (concat nodes-file
                                   nodes
                                   node)
                           vec)]
    (assoc parsed :options (-> options
                               (dissoc :node :nodes-file)
                               (assoc  :nodes all-nodes)))))

(defn rename-keys
  "Given a map m, and a map of keys to replacement keys, yields m with keys
  renamed."
  [m replacements]
  (reduce (fn [m [k k']]
            (-> m
                (assoc k' (get m k))
                (dissoc k)))
          m
          replacements))

(defn rename-options
  "Like rename-keys, but takes a parsed map and updates keys in :options."
  [parsed replacements]
  (update parsed :options rename-keys replacements))

(defn rename-ssh-options
  "Takes a parsed map and moves SSH options to a map under :ssh."
  [parsed]
  (let [{:keys [no-ssh
                username
                password
                strict-host-key-checking
                ssh-private-key]} (:options parsed)]
    (assoc parsed :options
           (-> (:options parsed)
               (assoc :ssh {:dummy?                    (boolean no-ssh)
                            :username                  username
                            :password                  password
                            :strict-host-key-checking  strict-host-key-checking
                            :private-key-path          ssh-private-key})
               (dissoc :no-ssh
                       :username
                       :password
                       :strict-host-key-checking
                       :private-key-path)))))


(defn test-opt-fn
  "An opt fn for running simple tests. Remaps ssh keys, remaps :node to :nodes,
  reads :nodes-file into :nodes, and parses :concurrency."
  [parsed]
  (-> parsed
      rename-ssh-options
      (rename-options {:leave-db-running :leave-db-running?})
      (rename-options {:logging-json :logging-json?})
      parse-nodes
      parse-concurrency))

;; Test runner

(defn run!
  "Parses arguments and runs tests, etc. Takes a map of subcommand names to
  subcommand-specs, and a list of arguments. Each subcommand-spec is a map with
  the following keys:

  :opt-spec       - The option parsing spec to use.
  :opt-fn         - A function to transform the tools.cli options map, e.g.
                    {:options ..., :arguments ..., :summary ...}. Default:
                    identity
  :usage          - A usage string (default: \"Usage:\")
  :run            - Function to execute with the transformed options
                    (default: pprint)

  If an unrecognized (or no command) is given, prints out a general usage guide
  and exits.

  For a subcommand, if help or --help is given, prints out a help string with
  usage for the given subcommand and exits with status 0.

  If invalid arguments are given, prints those errors to the console, and exits
  with status 254.

  Finally, if everything looks good, calls the given subcommand's `run`
  function with parsed options, and exits with status 0.

  Catches exceptions, logs them to the console, and exits with status 255."
  [subcommands [command & arguments :as argv]]
  (try
    (assert (not (get subcommands "--help")))
    (assert (not (get subcommands "help")))

    ; Top level help
    (when-not (get subcommands command)
      (println "Usage: lein run -- COMMAND [OPTIONS ...]")
      (print "Commands: ")
      (println (str/join ", " (sort (keys subcommands))))
      (System/exit 254))

    (let [{:keys [opt-spec opt-fn usage run]} (get subcommands command)
          opt-fn (or opt-fn identity)
          usage  (or usage (str "Usage: lein run -- " command
                                " [OPTIONS ...]"))
          run    (or run (fn [{:keys [options arguments summary errors]}]
                           (println "Arguments:")
                           (pprint arguments)
                           (println "\nOptions:")
                           (pprint options)
                           (println "\nErrors:")
                           (pprint errors)
                           (System/exit 0)))]

      ; Parse arguments
      (let [{:keys [options arguments summary errors] :as parsed-opts}
            (-> arguments
                (cli/parse-opts opt-spec)
                (update :options assoc :argv argv)
                opt-fn)]

        ; Subcommand help
        (when (:help options)
          (println usage)
          (println)
          (println summary)
          (System/exit 0))

        ; Bad args?
        (when (seq errors)
          (dorun (map println errors))
          (System/exit 254))

        ; Run!
        (run parsed-opts)
        (System/exit 0)))

    (catch Throwable t
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))

(defn serve-cmd
  "A web server command."
  []
  {"serve" {:opt-spec [help-opt
                       ["-b" "--host HOST" "Hostname to bind to"
                        :default "0.0.0.0"]
                       ["-p" "--port NUMBER" "Port number to bind to"
                        :default 8080
                        :parse-fn #(Long/parseLong %)
                        :validate [pos? "Must be positive"]]]
            :opt-fn #(update % :options rename-keys {:host :ip})
            :run (fn [{:keys [options]}]
                   (web/serve! options)
                   (info (str "Listening on http://"
                              (:ip options) ":" (:port options) "/"))
                   (loop [] (do
                              (Thread/sleep 1000)
                              (recur))))}})

(defn single-test-cmd
  "A command which runs a single test with standard built-ins. Options:

  {:opt-spec A vector of additional options for tools.cli. Merge into
             `test-opt-spec`. Optional.
   :opt-fn   A function which transforms parsed options. Composed after
             `test-opt-fn`. Optional.
   :opt-fn*  Replaces test-opt-fn, in case you want to override it altogether.
   :tarball If present, adds a --tarball option to this command, defaulting to
            whatever URL is given here.
   :usage   Defaults to `jc/test-usage`. Optional.
   :test-fn A function that receives the option map and constructs a test.}

  This comes with two commands: `test`, which runs a test and analyzes it, and
  `analyze`, which constructs a test map using the same arguments as `run`, but
  analyzes a history from disk instead.
  "
  [opts]
  (let [opt-spec (merge-opt-specs test-opt-spec (:opt-spec opts))
        opt-spec (if-let [default-tarball (:tarball opts)]
                   (conj opt-spec
                         [nil "--tarball URL" "URL for the DB tarball to install. May be either HTTP, HTTPS, or a local file on each DB node. For instance, --tarball https://foo.com/bar.tgz, or file:///tmp/bar.tgz"
                          :default default-tarball
                          :validate [(partial re-find #"^(file|https?)://.*\.(tar\.gz|tgz)")
                                     "Must be a file://, http://, or https:// URL ending in .tar.gz or .tgz"]])
                   opt-spec)
        opt-fn  (if (:tarball opts)
                  (comp test-opt-fn validate-tarball)
                  test-opt-fn)
        opt-fn  (if-let [f (:opt-fn opts)]
                  (comp f opt-fn)
                  opt-fn)
        opt-fn  (or (:opt-fn* opts) opt-fn)
        test-fn (:test-fn opts)]
  {"test" {:opt-spec opt-spec
           :opt-fn   opt-fn
           :usage    (:usage opts test-usage)
           :run      (fn [{:keys [options]}]
                       (info "Test options:\n"
                             (with-out-str (pprint options)))
                       (doseq [i (range (:test-count options))]
                         (let [test (jepsen/run! (test-fn options))]
                           (case (:valid? (:results test))
                             false    (System/exit 1)
                             :unknown (System/exit 2)
                             nil))))}

   "analyze"
   {:opt-spec [["-t" "--test INDEX_OR_PATH" "Index (e.g. -1 for most recent) or path to a Jepsen test file."
                :default  -1
                :parse-fn (fn [s]
                            (if (re-find #"^-?\d+$" s)
                              (Long/parseLong s)
                              s))]]
    :opt-fn   identity
    :usage    (:usage opts test-usage)
    :run      (fn [{:keys [options]}]
                (let [stored-test (store/test (:test options))
                      _ (info "Analyzing" (.getPath (store/path stored-test)))
                      argv (:argv stored-test)
                      _ (info "CLI args were" argv)
                      _ (assert+ (#{"test" "test-all"} (first argv))
                                 IllegalArgumentException
                                 (str "Not sure how to reconstruct test map from CLI args " (str/join " " argv)))
                      _ (assert+ (map? stored-test)
                                 IllegalStateException
                                 "Unable to load test")
                      ; Reparse original CLI options as if it had been a test
                      ; cmd
                      {:keys [options arguments summary errors] :as parsed-opts}
                      (-> (next argv) (cli/parse-opts opt-spec) opt-fn)
                      ; And construct a test from those opts
                      cli-test    (test-fn options)
                      test (-> cli-test
                               (merge (dissoc stored-test :results))
                               (vary-meta merge (meta stored-test)))]

                  (binding [*print-length* 32]
                    (info "Combined test:\n"
                          (-> test
                              (update :history (partial take 5))
                              (update :history vector)
                              (update :history conj '...)
                              pprint
                              with-out-str)))
                  (store/with-handle [test test]
                    (jepsen/analyze! test))))}}))

(defn test-all-run-tests!
  "Runs a sequence of tests and returns a map of outcomes (e.g. true, :unknown,
  :crashed, false) to collections of test folders with that outcome."
  [tests]
  (->> tests
       (map-indexed
         (fn [i test]
           (let [test' (jepsen/prepare-test test)]
             (try
               (let [test' (jepsen/run! test')]
                 [(:valid? (:results test'))
                  (.getPath (store/path test'))])
               (catch Exception e
                 (warn e "Test crashed")
                 [:crashed (.getPath (store/path test'))])))))
       (group-by first)
       (map-vals (partial map second))))

(defn test-all-print-summary!
  "Prints a summary of test outcomes. Takes a map of statuses (e.g. :crashed,
  true, false, :unknown), to test files. Returns results."
  [results]
  (println "\n")

  (when (seq (results true))
    (println "\n# Successful tests\n")
    (dorun (map println (results true))))

  (when (seq (results :unknown))
    (println "\n# Indeterminate tests\n")
    (dorun (map println (results :unknown))))

  (when (seq (results :crashed))
    (println "\n# Crashed tests\n")
    (dorun (map println (results :crashed))))

  (when (seq (results false))
    (println "\n# Failed tests\n")
    (dorun (map println (results false))))

  (println)
  (println (count (results true)) "successes")
  (println (count (results :unknown)) "unknown")
  (println (count (results :crashed)) "crashed")
  (println (count (results false)) "failures")

  results)

(defn test-all-exit!
  "Takes a map of statuses and exits with an appropriate error code: 255 if any
  crashed, 2 if any were unknown, 1 if any were invalid, 0 if all passed."
  [results]
  (System/exit (cond
                 (:crashed results)   255
                 (:unknown results)   2
                 (get results false)  1
                 true                 0)))

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go. Options:

    :opt-spec     A vector of additional options for tools.cli. Appended to
                  test-opt-spec. Optional.
    :opt-fn       A function which transforms parsed options. Composed after
                  test-opt-fn. Optional.
    :opt-fn*      Replaces test-opt-fn, instead of composing with it.
    :usage        Defaults to `test-usage`. Optional.
    :tests-fn     A function that receives the transformed option map and
                  constructs a sequence of tests to run."
  [opts]
  (let [opt-spec (merge-opt-specs test-opt-spec (:opt-spec opts))
        opt-fn  test-opt-fn
        opt-fn  (if-let [f (:opt-fn opts)]
                  (comp f opt-fn)
                  opt-fn)
        opt-fn  (or (:opt-fn* opts) opt-fn)]
    {"test-all"
     {:opt-spec opt-spec
      :opt-fn   opt-fn
      :usage    "Runs all tests"
      :run      (fn run [{:keys [options]}]
                  (info "CLI options:\n" (with-out-str (pprint options)))
                  (->> options
                       ((:tests-fn opts))
                       test-all-run-tests!
                       test-all-print-summary!
                       test-all-exit!))}}))

(defn -main
  [& args]
  (run! (serve-cmd)
        args))
