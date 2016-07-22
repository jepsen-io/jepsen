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
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach [register :as register]
                              [nemesis :as cln]]))

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (pr-str (sort (map name (if (map? coll) (keys coll) coll))))))

(def tests
  "A map of test names to test constructors."
  {"register"    register/test})

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
   "big-skews"                  `cln/bigskews
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

(def optspec
  "Command line options for tools.cli"
  [[nil "--force-download" "Always download HTTP/S tarballs"]

   ["-h" "--help" "Print out this message and exit"]

   ["-l" "--linearizable" "Whether to run cockroack in linearizable mode"
    :default false]

   ["-n" "--node HOSTNAME" "Node(s) to run test on"
    :default default-hosts
    :assoc-fn (fn [m k v] (if (identical? (get m k) default-hosts)
                            (assoc m k [v]) ; Replace with given host
                            (update m k conj v)))]

   [nil "--nemesis NAME" "Which nemesis to use"
    :default cln/none
    :parse-fn nemeses
    :validate [identity (one-of nemeses)]]

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

   ["-c" "--test-count NUMBER"
    "How many times should we repeat a test?"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-t" "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--username USER" "Username for logins"
    :default "root"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   ["-u" "--tarball URL" "URL for the Cockroach tarball to install. May be either HTTP, HTTPS, or a local file present on each of the DB nodes. For instance, --tarball https://foo.com/cockroach.tgz, or file:///tmp/cockroach.tgz"
    :default "https://binaries.cockroachdb.com/cockroach-beta-20160721.linux-amd64.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]])

(def usage
  (str "Usage: java -jar jepsen.cockroach.jar TEST-NAME [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  254   Invalid arguments
  255   Internal Jepsen error

Test names: " (str/join ", " (keys tests))
       "\n\nOptions:\n"))

(defn validate-test-name
  "Takes a tools.cli result map, and adds an error if the given
  arguments don't specify a valid test. Associates :test-fn otherwise."
  [parsed]
  (if (= 1 (count (:arguments parsed)))
    (let [test-name (first (:arguments parsed))]
      (if-let [f (get tests test-name)]
        (assoc parsed :test-fn f)
        (update parsed :errors conj
                (str "I don't know how to run " test-name
                     ", but I do know about "
                     (str/join ", " (keys tests))))))
    (update parsed :errors conj
            (str "No test name was provided. "
                 (one-of tests)))))

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

(defn -main
  [& args]
  (try
    (let [{:keys [options
                  arguments
                  summary
                  errors
                  test-fn]} (-> args
                               (cli/parse-opts optspec)
                               validate-test-name
                               validate-tarball)
          options (rename-keys options {:node :nodes})]

      ; Help?
      (when (:help options)
        (println usage)
        (println summary)
        (System/exit 0))

      ; Bad args?
      (when-not (empty? errors)
        (dorun (map println errors))
        (System/exit 254))

      (println "Test:\n" (with-out-str (pprint (test-fn options))))

      (dotimes [i (:test-count options)]
        (move-logfile!)
        ; Run test!
        (try
          ; Rehydrate test
          (let [test (-> options
                         ; Construct a fresh nemesis
                         (update :nemesis eval)
                         ; Construct test
                         test-fn
                         ; Run!
                         jepsen/run!)]
            (when-not (:valid? (:results test))
              (move-logfile!)
              (System/exit 1)))
          (catch com.jcraft.jsch.JSchException e
            (info e "SSH connection fault; aborting test and moving on")
            (move-logfile!))))

      (move-logfile!)
      (System/exit 0))
    (catch Throwable t
      (move-logfile!)
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))

