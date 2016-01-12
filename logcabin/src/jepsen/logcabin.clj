(ns jepsen.logcabin
  "Tests for LogCabin"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [cheshire.core :as json]))

(defn install!
  "Install LogCabin"
  [node]
  (info node "installing LogCabin")
  (debian/install [:git-core
                   :protobuf-compiler
                   :libprotobuf-dev
                   :libcrypto++-dev
                   :g++
                   :scons])
  (c/su
    (c/cd "/"
          (when-not (cu/file? "logcabin")
            (info node "git clone logcabin")
            (c/exec :git :clone :--depth 1 "https://github.com/logcabin/logcabin.git")
            (c/cd "/logcabin"
                  (c/exec :git :submodule :update :--init))))
    (c/cd "/logcabin"
          (info node "building logcabin")
          (c/exec :scons))
    (c/exec :cp :-f "/logcabin/build/LogCabin" "/root")
    (c/exec :cp :-f "/logcabin/build/Examples/Reconfigure" "/root")
    (c/exec :cp :-f "/logcabin/build/Examples/TreeOps" "/root")
    (info node "install LogCabin ok")))

(defn server-id
  [node]
  (str/replace (name node) "n" ""))

(defn server-addr
  [node]
  (str (name node) ":5254"))

(def config-file "/root/logcabin.conf")
(def log-file "/root/logcabin.log")
(def pid-file "/root/logcabin.pid")
(def store-dir "/root/storage")
(def server-addrs "n1:5254,n2:5254,n3:5254,n4:5254,n5:5254")
(def logcabin-bin "/root/LogCabin")
(def reconfigure-bin "/root/Reconfigure")
(def treeops-bin "/root/TreeOps")

(defn configure!
  "Configure LogCabin"
  [node]
  (info node "configuring LogCabin")
  (c/su
    (c/exec :echo (str 
                    "serverId = " 
                    (server-id node) 
                    "\n" 
                    "listenAddresses = " 
                    (server-addr node))
            :> config-file)))

(defn bootstrap!
  "bootstrap LogCabin"
  [node]
  (info node "bootstrapping LogCabin")
  (c/su
    (c/cd "/root"
          (c/exec logcabin-bin :-c config-file :-l log-file :--bootstrap))))

(defn start!
  "Start LogCabin"
  [node]
  (info node "starting LogCabin")
  (c/su
    (c/cd "/root"
          (c/exec logcabin-bin :-c config-file :-d :-l log-file :-p pid-file))))

(defn stop!
  "Stop LogCabin"
  [node]
  (info node "stopping LogCabin")
  (c/su
    (cu/grepkill! :LogCabin)
    (c/exec :rm :-rf pid-file)))

(defn reconfigure!
  "Reconfigure LogCabin servers"
  [node]
  (info node "reconfiguring LogCabin servers")
  (c/su
    (c/cd "/root"
          (c/exec reconfigure-bin 
                  :-c (c/lit server-addrs)
                  :set 
                  (c/lit (server-addr :n1))
                  (c/lit (server-addr :n2))
                  (c/lit (server-addr :n3))
                  (c/lit (server-addr :n4))
                  (c/lit (server-addr :n5))))))

(defn db
  "Sets up and tears down LogCabin"
  []
  (reify db/DB
    (setup! [_ test node]
            (info node "set up")
            (install! node)
            (configure! node)
            
            ; Remove log file first.
            (c/exec :rm :-rf log-file)
            
            ; Bootstrap in n1.
            (when (= node :n1)
              (bootstrap! node))
            
            (jepsen/synchronize test)
            (start! node)
            
            (jepsen/synchronize test)
            ; Use Reconfigure tool to set up the LogCabin cluster in n1.
            (when (= node :n1)
              (reconfigure! node))
            
            (jepsen/synchronize test)
            (Thread/sleep 2000))
    
    (teardown! [_ test node]
               (stop! node)
               
               (c/su 
                 (c/exec :rm :-rf store-dir))
               
               (info node "tore down"))))

(def cas-msg-pattern
  "LogCabin returns following error for CAS failed"
  (re-pattern "Exiting due to LogCabin::Client::Exception: Path '.*' has value '.*', not '.*' as required"))

(def timeout-msg-pattern
  (re-pattern "Exiting due to LogCabin::Client::Exception: Client-specified timeout elapsed"))

(def op-timeout 
  "Use 3s for LogCabin operation timeout"
  3)

(defn logcabin-get! 
  "Get a value for path"
  [node path]
  (c/on node
        (c/su 
          (c/cd "/root"
                (c/exec treeops-bin
                        :-c server-addrs
                        :-q
                        :-t op-timeout
                        :read
                        (c/lit path))))))

(defn logcabin-set! 
  "Set a value for path"
  [node path value]
  (c/on node 
        (c/su 
          (c/cd "/root"
                (c/exec :echo :-n value | 
                        treeops-bin
                        :-c server-addrs
                        :-q
                        :-t op-timeout
                        :write
                        (c/lit path))))))

(defn logcabin-cas!
  "Set value2 for path if old value is value1"
  [node path value1 value2]
  (try 
    (c/on node
          (c/su 
            (c/cd "/root"
                  (c/exec :echo :-n value2 | 
                          treeops-bin
                          :-c server-addrs
                          :-q
                          :-p (c/lit (str path ":" value1))
                          :-t op-timeout
                          :write
                          (c/lit path)))))
    (catch Exception e
      ; For CAS failed, we return false, otherwise, re-raise the error. 
      (if (re-matches cas-msg-pattern (str/trim (.getMessage e)))
        false
        (throw e)))))


(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
          (let [n node]
            (logcabin-set! node k (json/generate-string nil))
            (assoc this :client node)))
  
  (invoke! [this test op]
           (try
             (case (:f op)  
               :read  (let [resp (-> client
                                     (logcabin-get! k)
                                     (json/parse-string true))]
                        (assoc op :type :ok :value resp))
               :write (do (->> (:value op)
                               json/generate-string
                               (logcabin-set! client k))
                        (assoc op :type :ok))
               
               :cas   (let [[value value'] (:value op)
                            ok?            (logcabin-cas! client k
                                                          (json/generate-string value)
                                                          (json/generate-string value'))]
                        (assoc op :type (if ok? :ok :fail))))
             (catch Exception e
               (let [msg (str/trim (.getMessage e))]
                 (cond 
                   (re-matches timeout-msg-pattern msg) (assoc op :type :fail :value :timed-out)
                   :else (throw e))))))
  
  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single LogCabin node."
  []
  (CASClient. "/jepsen" nil))