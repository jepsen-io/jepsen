(ns jepsen.cockroach.runner
  "Runs CockroachDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.core :as jepsen]
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach [register :as register]]))

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (pr-str (sort (map name (if (map? coll) (keys coll) coll))))))

(def tests
  "A map of test names to test constructors."
  {"multi"       multi/multi-test
   "single"      single/single-test
   "dirty-read"  dirty-read/dirty-read-test})

(def default-hosts [:n1 :n2 :n3 :n4 :n5])

(def optspec
  "Command line options for tools.cli"
  [["-h" "--help" "Print out this message and exit"]

   ["-n" "--node HOSTNAME" "Node(s) to run test on"
    :default default-hosts
    :assoc-fn (fn [m k v] (if (identical? (get m k) default-hosts)
                            (assoc m k [v]) ; Replace with given host
                            (update m k conj v)))]

   ["-t" "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--recovery-delay SECONDS"
    "How long should we wait before killing nodes and recovering?"
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-c" "--test-count NUMBER"
    "How many times should we repeat a test?"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   [nil "--strong-reads" "Use stored procedure including a write for all reads"]

   [nil "--skip-os" "Don't perform OS setup"]

   [nil "--force-download" "Always download HTTP/S tarballs"]

   [nil "--username USER" "Username for logins"
    :default "root"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   [nil "--password PASS" "Password for sudo access"
    :default "root"
    :assoc-fn (fn [m k v] (assoc-in m [:ssh k] v))]

   ["-p" "--procedure-call-timeout MILLISECONDS"
    "How long should we wait before timing out procedure calls?"
    :default 1000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-u" "--tarball URL" "URL for the Mongo tarball to install. May be either HTTP, HTTPS, or a local file. For instance, --tarball https://foo.com/mongo.tgz, or file:///tmp/mongo.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]])

(def usage
  (str "Usage: java -jar jepsen.voltdb.jar TEST-NAME [OPTIONS ...]

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
            (str "No test name was provided. Use one of "
                 (str/join ", " (keys tests))))))


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
          options (rename-keys options {:strong-reads     :strong-reads?
                                        :no-reads         :no-reads?
                                        :force-download   :force-download?
                                        :skip-os          :skip-os?
                                        :node             :nodes})]

      ; Help?
      (when (:help options)
        (println usage)
        (println summary)
        (System/exit 0))

      ; Bad args?
      (when-not (empty? errors)
        (dorun (map println errors))
        (System/exit 254))

      (println "Test options:\n" (with-out-str (pprint options)))

      (dotimes [i (:test-count options)]
        (move-logfile!)
        ; Run test!
        (try
          (when-not (:valid? (:results (jepsen/run! (test-fn options))))
            (move-logfile!)
            (System/exit 1))
          (catch com.jcraft.jsch.JSchException e
            (info e "SSH connection fault; aborting test and moving on")
            (move-logfile!))))

      (move-logfile!)
      (System/exit 0))
    (catch Throwable t
      (move-logfile!)
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))

