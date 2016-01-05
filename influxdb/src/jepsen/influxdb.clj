(ns jepsen.influxdb

 "Tests for InfluxDB"
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
            [jepsen.os.debian :as debian])
  (:import (org.influxdb InfluxDB InfluxDBFactory))
  (:import (org.influxdb.dto Point Query))
  (:import (java.util.concurrent.TimeUnit))
  )

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
	
(defn consToRef [element theRef]    
    (dosync
        (let [newSeq  (cons element (deref theRef))]
           (ref-set theRef newSeq)        
           newSeq
           )
      )
)

(defn nodesToJoin [nodes]
  (if (>= 1 (count nodes)) "" 
    (str "-join " (clojure.string/join "," (map (fn [n] (str (name n) ":8088")) (rest nodes)))))   
)

(defn connect [node]
    (let [
            c (InfluxDBFactory/connect (str "http://" (name node) ":8086") "root" "root")
            initialPoint (.build (.field (.time (Point/measurement "answers") 1 java.util.concurrent.TimeUnit/NANOSECONDS) "answer" 42))
          ]
   
      (.enableBatch c 1 1 java.util.concurrent.TimeUnit/MILLISECONDS)
      (.setLogLevel c org.influxdb.InfluxDB$LogLevel/FULL)
      (.createDatabase c "jepsen")
      (.write c "jepsen", "default", initialPoint)
      c
    )
)

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (info node "setting up client")
      (client (connect node)))

    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
           (case (:f op)
             :read (assoc op :type :ok, :value 
                (let [q (Query. "SELECT answer from answers where time = 1" "jepsen")]
                    (-> conn 
                        (.query q) 
                        (.getResults)
                        (.get 0)
                        (.getSeries)
                        (.get 0)
                        (.getValues)
                        (.get 0)
                        (.get 1)
                      )
                  )
              ))

      )
    )
    (teardown! [_ test])))

(defn db
  "Sets up and tears down InfluxDB"
  [version]
   (let [nodeOrderRef (ref [])]
    (reify db/DB
      (setup! [_ test node]
        (try
          (c/su
          (info node "Stopping influxdb...")
          (meh (c/exec :killall :-9 "influxd"))
          (c/exec :service :influxdb :stop)
          (info node "Removing influxdb...")
          (c/exec :dpkg :--purge "influxdb")
          (info node "Removed influxdb")

          )
          (catch Throwable _)
        )

       (info node "Starting influxdb setup.")
       (try    
        (c/cd "/tmp"
              (let [file (str "influxdb_" version "_amd64.deb")]
                (when-not (cu/file? file)
                  (info "Fetching deb package from influxdb repo")
                  (c/exec :wget (str "https://s3.amazonaws.com/influxdb/" file)))

                (c/su
                  

                  ; Install package
                  (try (c/exec :dpkg-query :-l :influxdb)
                       (catch RuntimeException _
                         (info "Installing influxdb...")                    
                         (c/exec :dpkg :-i file)))))
                (c/su
                	(c/exec :cp "/usr/lib/influxdb/scripts/init.sh" "/etc/init.d/influxdb")

                	; preparing for the clustering, hostname should be the node's name
                    (info node "Copying influxdb configuration...")              
                	(c/exec :echo (-> (io/resource "influxdb.conf")
                          slurp
                          (str/replace #"%HOSTNAME%" (name node)))
                     :> "/etc/influxdb/influxdb.conf")


                      
                    ; first stab at clustering -- doesn't really work yet
                	(c/exec :echo 
                		(-> (io/resource "servers.sh")  slurp) :> "/root/servers.sh")
                  (c/exec :echo 
                    (-> (io/resource "test_cluster.sh")  slurp) :> "/root/test_cluster.sh")
                      (c/exec :echo 
                    (-> (io/resource "test_influx_up.sh")  slurp) :> "/root/test_influx_up.sh")

                 
                	(try (c/exec :service :influxdb :stop)
                        (catch Exception _ 
                          (info node "no need to stop")
                        )
                  )

                  ; clearing out clustering info
                  (c/exec :rm :-rf "/var/lib/influxdb/meta/*")
                 )
              
              (info node "I am waiting for the lock...")
               (locking nodeOrderRef 
                  (info node "I have the lock!")
                  (let [norder (consToRef node nodeOrderRef)]                              
                   
                       (info node "nodes to join: " (nodesToJoin norder) norder (deref nodeOrderRef) )    
                       (let [joinParams (str "'" (-> (io/resource "influxdb")  slurp 
                            (str/replace #"%NODES_TO_JOIN%" (nodesToJoin norder))) "'")]
                         (info node "joinParams: " joinParams)
                          (c/su 
                            (c/exec :echo
                              joinParams
                              :>
                              "/etc/default/influxdb"
                              )   
                           )   
                       ) 
                 		    ; Ensure node is running
                       (try (c/exec :service :influxdb :status)
                           (catch RuntimeException _
                           (info node "Starting influxdb...")
                           (c/exec :service :influxdb :start)))
                      
                        (info node "InfluxDB started!")                    
                   )
                 )

               (while 
                (try (c/exec :bash "/root/test_cluster.sh") 
                        false
                     (catch Exception _
                        true
                      )
                 )
                 (do 
                      (info node "waiting for influx to start...")
                      (Thread/sleep 1000)
                  )
                )
               (jepsen/synchronize test)
               (c/exec :bash "/root/test_cluster.sh") 
               (info node "This node is OK, sees 3 members in the raft cluster")
  		      )
  	      (catch RuntimeException e
  	      		(error node "Error at Setup: " e (.getMessage e))
  	      		(throw e)
  	      	)
  	      )
        )


      (teardown! [_ test node]
      	;; nothing for now for examination of running nodes
       ))

      )
    )

(defn basic-test
  "A simple test of InfluxDB's safety."
  [version]
  (merge tests/noop-test 
  	 {:ssh 
  	 	{ :username "root"
  	 	  :private-key-path "~/.ssh/id_rsa"
  	 	}      
  	 	:nodes     [:n1 :n2 :n3 ]
  	   :concurrency 3
  	   :os debian/os
       :client (client nil)
       :db (db version)
       :generator (->> r
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))
  	 }
  	)
 )
