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
            [jepsen [cli :as jc]
                    [core :as jepsen]]))

(def opt-spec
  "Command line option specification for tools.cli."
  [[nil "--key-time-limit SECONDS"
    "How long should we test an individual key for, in seconds?"
    :default  30
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-w" "--write-concern LEVEL" "Write concern level"
    :default  :majority
    :parse-fn keyword
    :validate [client/write-concerns (jc/one-of client/write-concerns)]]

   ["-r" "--read-concern LEVEL" "Read concern level"
    :default  :linearizable
    :parse-fn keyword
    :validate [client/read-concerns (jc/one-of client/read-concerns)]]

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
    :default  "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian81-3.4.0-rc2.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar\.gz|tgz)")
               "Must be a file://, http://, or https:// URL ending in .tar.gz or .tgz"]]
  ])

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn   #(-> % (jc/rename-options {:node :nodes}))
           :usage    jc/test-usage
           :run      (fn [{:keys [options]}]
                       (info "Test options:\n" (with-out-str (pprint options)))

                       ; Run test
                       (doseq [i (range (:test-count options))]
                         (let [test (jepsen/run! (dc/test options))]
                           (when-not (:valid? (:results test))
                             (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
