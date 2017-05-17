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
            [jepsen.core :as jepsen]
            [jepsen.web :as web]))

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

(def help-opt
  ["-h" "--help" "Print out this message and exit"])

(def test-opt-spec
  "Command line options for testing."

  [help-opt

   (repeated-opt "-n" "--node HOSTNAME" "Node(s) to run test on" default-nodes)

   [nil "--nodes-file FILENAME" "File containing node hostnames, one per line."]

   [nil "--username USER" "Username for logins"
    :default "root"]

   [nil "--password PASS" "Password for sudo access"
    :default "root"]

   [nil "--strict-host-key-checking" "Whether to check host keys"
    :default false]

   [nil "--ssh-private-key FILE" "Path to an SSH identity file"]

   [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "1n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

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

(defn tarball-opt
  [default]
  ["-u" "--tarball URL" "URL for the DB tarball to install. May be either HTTP, HTTPS, or a local file present on each of the DB nodes. For instance, --tarball https://foo.com/db.tgz, or file:///tmp/db.tgz"
   :default default
   :validate [(partial re-find #"^(file|https?)://.*\.(tar|tgz)")
              "Must be a file://, http://, or https:// URL including .tar or .tgz"]])

(defn test-usage
  []
  "Usage: lein run -- COMMAND [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
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
  integer."
  [parsed]
  (let [c (:concurrency (:options parsed))]
    (let [[match integer unit] (re-find #"(\d+)(n?)" c)]
      (when-not match
        (throw (IllegalArgumentException.
                 (str "--concurrency " c
                      " should be an integer optionally followed by n"))))
      (let [unit (if (= "n" unit)
                   (count (:nodes (:options parsed)))
                   1)]
        (assoc-in parsed [:options :concurrency]
                  (* unit (Long/parseLong integer)))))))

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
  (let [{:keys [username
                password
                strict-host-key-checking
                ssh-private-key]} (:options parsed)]
    (assoc parsed :options
           (-> (:options parsed)
               (assoc :ssh {:username                  username
                            :password                  password
                            :strict-host-key-checking  strict-host-key-checking
                            :private-key-path          ssh-private-key})
               (dissoc :username
                       :password
                       :strict-host-key-checking
                       :private-key-path)))))

(defn read-nodes-file
  "Takes a parsed map. If :nodes-file exists, reads its contents and appends
  them to :nodes. Drops the default nodes list if it's still there."
  [parsed]
  (let [options (:options parsed)]
    (assoc parsed :options
           (if-let [f (:nodes-file options)]
             (let [nodes (:nodes options)
                   nodes (if (identical? nodes default-nodes)
                           []
                           nodes)
                   nodes (into nodes (str/split (slurp f) #"\s*\n\s*"))]
               (assoc options :nodes nodes))
             options))))

(defn test-opt-fn
  "An opt fn for running simple tests. Remaps ssh keys, remaps :node to :nodes,
  reads :nodes-file into :nodes, and parses :concurrency."
  [parsed]
  (-> parsed
      (rename-options {:node :nodes})
      rename-ssh-options
      read-nodes-file
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
  [subcommands [command & arguments]]
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
                   (while true (Thread/sleep 1000)))}})

(defn single-test-cmd
  "A command which runs a single test with standard built-ins. Options:

  {:opt-spec A vector of additional options for tools.cli. Appended to
             `test-opt-spec`. Optional.
   :opt-fn   A function which transforms parsed options. Composed after
             `test-opt-fn`. Optional.
   :tarball If present, adds a --tarball option to this command, defaulting to
            whatever URL is given here.
   :usage   Defaults to `jc/test-usage`. Optional.
   :test-fn A function that receives the option map and constructs a test.}"
  [opts]
  (let [opt-spec (into test-opt-spec (:opt-spec opts))
        opt-spec (if-let [default-tarball (:tarball opts)]
                   (conj opt-spec
                         [nil "--tarball URL" "URL for the DB tarball to install. May be either HTTP, HTTPS, or a local file on each DB node. For instance, --tarball https://foo.com/bar.tgz, or file:///tmp/bar.tgz"
                          :default default-tarball
                          :validate [(partial re-find #"^(file|https?)://.*\.(tar\.gz|tgz)")
                                     "Must be a file://, http://, or https:// URL ending in .tar.gz or .tgz"]])
                   opt-spec)
        opt-fn  (if (:tarball opts)
                  (comp test-opt-fn validate-tarball)
                  test-opt-fn)]
  {"test" {:opt-spec opt-spec
           :opt-fn   (if-let [f (:opt-fn opts)]
                       (comp f opt-fn)
                       opt-fn)
           :usage    (:usage opts test-usage)
           :run      (fn [{:keys [options]}]
                       (info "Test options:\n"
                             (with-out-str (pprint options)))
                       (doseq [i (range (:test-count options))]
                         (let [test (jepsen/run! ((:test-fn opts) options))]
                           (when-not (:valid? (:results test))
                             (System/exit 1)))))}}))

(defn -main
  [& args]
  (run! (serve-cmd)
        args))
