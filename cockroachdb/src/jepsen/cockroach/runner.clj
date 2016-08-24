(ns jepsen.cockroach.runner
  "Runs CockroachDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.core :as jepsen]
            [jepsen.web :as web]
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach [bank :as bank]
                              [register :as register]
                              [monotonic :as monotonic]
                              [nemesis :as cln]
                              [sets :as sets]
                              [sequential :as sequential]]))

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (str/join ", " (sort (map name (if (map? coll) (keys coll) coll))))))

(def tests
  "A map of test names to test constructors."
  {"bank"                 bank/test
   "bank-multitable"      bank/multitable-test
   "register"             register/test
   "monotonic"            monotonic/test
   "monotonic-multitable" monotonic/multitable-test
   "sets"                 sets/test
   "sequential"           sequential/test})

(def default-hosts [:n1 :n2 :n3 :n4 :n5])

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "ubuntu" ubuntu/os
   "none"   os/noop})

(def nemeses
  "Supported nemeses"
  {"none"                       `cln/none
   "parts"                      `cln/parts
   "majority-ring"              `cln/majring
   "skews"                      `cln/skews
   "small-skews"                `cln/small-skews
   "big-skews"                  `cln/big-skews
   "huge-skews"                 `cln/huge-skews
   "start-stop"                 `(cln/startstop 1)
   "start-stop-2"               `(cln/startstop 2)
   "start-kill"                 `(cln/startkill 1)
   "start-kill-2"               `(cln/startkill 2)
   "skews-start-kill-2"         `(cln/compose cln/skews      (cln/startkill 2))
   "majority-ring-start-kill-2" `(cln/compose cln/majring    (cln/startkill 2))
   "parts-skews"                `(cln/compose cln/parts      cln/skews)
   "parts-big-skews"            `(cln/compose cln/parts      cln/bigskews)
   "parts-start-kill-2"         `(cln/compose cln/parts      (cln/startkill 2))
   "majority-ring-skews"        `(cln/compose cln/majring    cln/skews)
   "start-stop-skews"           `(cln/compose cln/startstop  cln/skews)})

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

(def optspec
  "Command line options for tools.cli"
  [[nil "--force-download" "Always download HTTP/S tarballs"]

   ["-h" "--help" "Print out this message and exit"]

   ["-l" "--linearizable" "Whether to run cockroack in linearizable mode"
    :default false]

   (repeated-opt "-n" "--node HOSTNAME" "Node(s) to run test on" default-hosts)

   (repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                 [`cln/none]
                 nemeses)

   ["-o" "--os NAME" "debian, ubuntu, or none"
    :default debian/os
    :parse-fn oses
    :validate [identity (one-of oses)]]

   [nil "--password PASS" "Password for sudo access"
    :default "root"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   [nil "--recovery-delay SECONDS"
    "How long should we wait before killing nodes and recovering?"
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--strict-host-key-checking" "Whether to check host keys"
    :default false
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   [nil "--ssh-private-key FILE" "Path to an SSH identity file"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh :private-key-path] v))]

   [nil "--concurrency NUMBER" "How many workers should we run?"
    :default 30
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-c" "--test-count NUMBER"
    "How many times should we repeat a test?"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (repeated-opt "-t" "--test NAME" "Test(s) to run"
                 []
                 tests)

   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default 60
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  60
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--username USER" "Username for logins"
    :default "root"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   ["-u" "--tarball URL" "URL for the Cockroach tarball to install. May be either HTTP, HTTPS, or a local file present on each of the DB nodes. For instance, --tarball https://foo.com/cockroach.tgz, or file:///tmp/cockroach.tgz"
    :default "https://binaries.cockroachdb.com/cockroach-beta-20160728.linux-amd64.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]])

(def usage
  (str "Usage: java -jar jepsen.cockroach.jar test [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  254   Invalid arguments
  255   Internal Jepsen error

Options:\n"))

(defn validate-tarball
  "Ensures a tarball is present."
  [parsed]
  (if (:tarball (:options parsed))
    parsed
    (update parsed :errors conj "No tarball URL provided")))

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

(defn move-logfile!
  "Moves jepsen.log to store/latest/"
  []
  (let [f  (io/file "jepsen.log")
        f2 (io/file "store/latest/jepsen.log")
  d1 (io/file "store/latest")]
    (when (and (.exists f) (.exists d1) (not (.exists f2)))
      ; Can't just rename because log4j retains the filehandle
      (io/copy f f2)
      (spit f "" :append false))))

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn -main
  [& args]
  (try
    (let [{:keys [options
                  arguments
                  summary
                  errors]} (-> args
                               (cli/parse-opts optspec)
                               validate-tarball)
          options (rename-keys options {:node    :nodes
                                        :nemesis :nemeses
                                        :test    :test-fns})]

      ; Help?
      (when (:help options)
        (println usage)
        (println summary)
        (System/exit 0))

      ; Server
      (when (= ["serve"] arguments)
        (web/serve! {})
        (while true (Thread/sleep 1000000)))

      ; Bad args?
      (when-not (empty? errors)
        (dorun (map println errors))
        (System/exit 254))

      (doseq [test-fn (:test-fns options)
              nemesis (:nemeses options)
              i       (range (:test-count options))]
        (move-logfile!)
        ; Rehydrate test and run
        (let [test (-> options
                       (dissoc :test-fns)               ; No longer needed
                       (assoc :nemesis (eval nemesis))  ; Construct new nemesis
                       test-fn                          ; Construct test
                       log-test
                       jepsen/run!)]                    ; Run!
          (when-not (:valid? (:results test))
            (move-logfile!)
            (System/exit 1))))

      (move-logfile!)
      (System/exit 0))
    (catch Throwable t
      (move-logfile!)
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))
