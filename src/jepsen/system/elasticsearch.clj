(ns jepsen.system.elasticsearch
  (:require [clojure.tools.logging  :refer [debug info warn]]
            [clojure.java.io        :as io]
            [clojure.string         :as str]
            [jepsen.core            :as core]
            [jepsen.util            :refer [meh timeout]]
            [jepsen.codec           :as codec]
            [jepsen.core            :as core]
            [jepsen.control         :as c]
            [jepsen.control.net     :as net]
            [jepsen.control.util    :as cu]
            [jepsen.client          :as client]
            [jepsen.db              :as db]
            [jepsen.generator       :as gen]
            [jepsen.os.debian       :as debian]
            [knossos.core           :as knossos]
            [cheshire.core          :as json]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.response :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]))

(defn wait
  "Waits for elasticsearch to be healthy on the current node. Color is red,
  yellow, or green; timeout is in seconds."
  [timeout-secs color]
  (timeout (* 1000 timeout-secs) :timed-out
    (loop []
      (when
        (try
          (c/exec :curl :-XGET
                  (str "http://localhost:9200/_cluster/health?"
                       "wait_for_status=" (name color)
                       "&timeout=" timeout-secs "s"))
          false
          (catch RuntimeException e true))
        (recur)))))

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (c/cd "/tmp"
              (let [version "1.1.0"
                    debfile (str "elasticsearch-" version ".deb")
                    uri     (str "https://download.elasticsearch.org/"
                                 "elasticsearch/elasticsearch/"
                                 debfile)]
                (when-not (debian/installed? :elasticsearch)
                  (loop []
                    (info node "downloading elasticsearch")
                    (c/exec :wget :-c uri)
                    (c/exec :wget :-c (str uri ".sha1.txt"))
                    (when (try
                            (c/exec :sha1sum :-c (str debfile ".sha1.txt"))
                            false
                            (catch RuntimeException e
                              (info "SHA failed" (.getMessage e))
                              true))
                      (recur)))

                  (info node "installing elasticsearch")
                  (c/exec :apt-get :install :-y :openjdk-7-jre-headless)
                  (c/exec :dpkg :-i :--force-confnew debfile))))

        (info node "configuring elasticsearch")
        (c/exec :echo
                (-> "elasticsearch/elasticsearch.yml"
                    io/resource
                    slurp
                    (str/replace "$HOSTS" (json/generate-string
                                            (vals (c/on-many (:nodes test)
                                                             (net/local-ip))))))
                :> "/etc/elasticsearch/elasticsearch.yml")

        (info node "starting elasticsearch")
        (c/exec :service :elasticsearch :restart))

      (wait 60 :green)
      (info node "elasticsearch ready"))

    (teardown! [_ test node]
      (c/su
        (meh (c/exec :service :elasticsearch :stop))
        (c/exec :rm :-rf "/var/lib/elasticsearch/elasticsearch"))
      (info node "elasticsearch nuked"))))

(defmacro with-es
  "Given a list of nodes, runs body with an elasticsearch client bound."
  [client & body]
  `(binding [es/*endpoint* ~client]
     ~@body))

(def index-name "jepsen-index")

(def mappings {"number"
               {:properties {:num {:type "integer" :store "yes"}}}})

(defn http-error
  "Takes an elastisch ExInfo exception and extracts the HTTP error response as
  a string."
  [ex]
  ; AFIACT this is the shortest path to actual information about what went
  ; wrong
  (-> ex .getData :object :body json/parse-string (get "error")))

(defn all-results
  "A sequence of all results from a search query."
  [& search-args]
  (let [res      (apply esd/search (concat search-args [:size 0]))
        count-from (esr/count-from res)
        hitcount (esr/total-hits res)
        res      (apply esd/search (concat search-args [:size hitcount]))]
    (info :count-from count-from)
    (info :total-hits hitcount)
    (when (esr/timed-out? res)
      (throw (RuntimeException. "Timed out")))
    (esr/hits-from res)))

(defrecord SetClient [client]
  client/Client
  (setup! [_ test node]
    (let [; client (es/connect [[(name node) 9300]])]
          client (es/connect (str "http://" (name node) ":9200"))]
      ; Create index
      (try
        (with-es client
          (esi/create index-name
                      :mappings mappings
                      :settings {"index" {"refresh_interval" "1"}}))
        (catch clojure.lang.ExceptionInfo e
          ; Is this seriously how you're supposed to do idempotent
          ; index creation? I've gotta be doing this wrong.
          (let [err (http-error e)]
            (when-not (re-find #"IndexAlreadyExistsException" err)
              (throw (RuntimeException. err))))))

      (SetClient. client)))

  (invoke! [this test op]
    (timeout 10000 (assoc op :type :info :value :timed-out)
      (with-es client
        (case (:f op)
          :add (let [r (esd/create index-name "number" {:num (:value op)})]
                 (cond (esr/ok? r)
                       (assoc op :type :ok)

                       (esr/timed-out? r)
                       (assoc op :type :info :value :timed-out)

                       :else
                       (assoc op :type :info :value r)))
          :read (try
                  (esi/flush index-name)
                  (assoc op :type :ok
                         :value (->> (all-results index-name "number")
                                     (map (comp :num :_source))
                                     set))
                  (catch RuntimeException e
                    (assoc op :type :fail :value (.getMessage e))))))))

  (teardown! [_ test]
    (.close client)))

(defn set-client []
  (SetClient. nil))
