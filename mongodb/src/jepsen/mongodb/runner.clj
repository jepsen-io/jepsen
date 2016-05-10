(ns jepsen.mongodb.runner
  "Runs the full Mongo test suite, including a config file. Provides exit
  status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen.mongodb [core :as m]
                            [mongo :as client]
                            [document-cas :as dc]]
            [jepsen.core :as jepsen]))

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (pr-str (sort (map name (if (map? coll) (keys coll) coll))))))

(def optspec
  "Command line option specification for tools.cli."
  [["-h" "--help" "Print out this message and exit"]

   ["-t" "--time-limit SECONDS"
    "Excluding setup and teardown, how long should tests run for, in seconds?"
    :default  150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--key-time-limit SECONDS"
    "How long should we test an individual key for, in seconds?"
    :default  30
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-w" "--write-concern LEVEL" "Write concern level"
    :default  :majority
    :parse-fn keyword
    :validate [client/write-concerns (one-of client/write-concerns)]]

   ["-r" "--read-concern LEVEL" "Read concern level"
    :default  :majority
    :parse-fn keyword
    :validate [client/read-concerns (one-of client/read-concerns)]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   [nil "--read-with-find-and-modify" "Use findAndModify to ensure read safety"]

   ["-s" "--storage-engine ENGINE" "Mongod storage engine"
    :default  "wiredTiger"
    :validate [(partial re-find #"\A[a-zA-Z0-9]+\z") "Must be a single word"]]

   ["-p" "--protocol-version INT" "Replication protocol version number"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]

   [nil "--tarball URL" "URL for the Mongo tarball to install. May be either HTTP, HTTPS, or a local file. For instance, --tarball https://foo.com/mongo.tgz, or file:///tmp/mongo.tgz"
    :default  "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian71-3.3.1.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar\.gz|tgz)")
               "Must be a file://, http://, or https:// URL ending in .tar.gz or .tgz"]]
  ])

(def usage
  "Usage: java -jar jepsen.mongodb.jar [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  254   Invalid arguments
  255   Internal Jepsen error

Options:\n")

(defn -main
  [& args]
  (try
    (let [{:keys [options arguments summary errors]}
          (cli/parse-opts args optspec)]
      ; Bad args?
      (when-not (empty? errors)
        (dorun (map println errors))
        (System/exit 254))

      ; Help?
      (when (:help options)
        (println usage)
        (println summary)
        (System/exit 0))

      (info "Test options:\n" (with-out-str (pprint options)))

      ; Run test
      (let [t (jepsen/run! (dc/test options))]
        (System/exit (if (:valid? (:results t)) 0 1))))

    (catch Throwable t
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))
