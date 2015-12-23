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
            [jepsen.os.debian :as debian]))
	

(defn db
  "Sets up and tears down InfluxDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
     (info node "Starting influxdb setup.")
     (try    
      (c/cd "/tmp"
            (let [version "0.9.6.1"
                  file (str "influxdb_" version "_amd64.deb")]
              (when-not (cu/file? file)
                (info "Fetching deb package from influxdb repo")
                (c/exec :wget (str "https://s3.amazonaws.com/influxdb/" file)))

              (c/su
                ; Install package
                (try (c/exec :dpkg-query :-l :influxdb)
                     (catch RuntimeException _
                       (info "Installing influxdb...")                    
                       (c/exec :dpkg :-i file)))))

            	(c/exec :cp "/usr/lib/influxdb/scripts/init.sh" "/etc/init.d/influxdb")

            	; preparing for the clustering, hostname should be the node's name
                (info node "Copying influxdb configuration...")              
            	(c/exec :echo (-> (io/resource "influxdb.conf")
                      slurp
                      (str/replace #"%HOSTNAME%" (name node)))
                 :> "/etc/influxdb/influxdb.conf")
                  
                ; first stab at clustering -- doesn't really work yet
            ;	(c/exec :echo 
            ;		(-> (io/resource "peers.json")  slurp) :> "/var/lib/influxdb/meta/peers.json")

              (c/exec :rm :-f "/var/lib/influxdb/meta/peers.json")
            	(try (c/exec :service :influxdb :stop)
                    (catch Exception _ 
                      (info node "no need to stop")
                    )
              )

            	
               ; (c/exec :echo 
               ; (-> (io/resource "infxludb")  slurp (str/replace #"%NODES_TO_JOIN%" (str (name node) ":8088")))  :> "/etc/default/influxdb")      

            	;(jepsen/synchronize test);

              (locking test 
                (dosync
                  (ref-set (:nodeOrder test) (conj (deref (:nodeOrder test)) node))
                  (info node "I have the lock! nodeOrder: " (:nodeOrder test))    
             		; Ensure node is running
                  (try (c/exec :service :influxdb :status)
                       (catch RuntimeException _
                       (info node "Starting influxdb...")
                       (c/exec :service :influxdb :start)))
                  
                  (info node "InfluxDB started!") 
                  )
              )

            )
		
	      (catch RuntimeException e
	      		(error node "Error at Setup: " e (.getMessage e))
	      		(throw e)
	      	)
	      )
      )


    (teardown! [_ test node]
    	(try
	      (c/su
	 		(info node "Stopping influxdb...")
	 		;(meh (c/exec :killall :-9 "influxd"))
	 		;(c/exec :service :influxdb :stop)
	 		(info node "Removing influxdb...")
	 		;(c/exec :dpkg :--purge "influxdb")
	 		(info node "Removed influxdb")
	       )
	      (catch RuntimeException e
	      		(error node "Error at TearDown: " e (.getMessage e))
	      		(throw e)
	      	)
      )
     ))


    )

(defn basic-test
  "A simple test of InfluxDB's safety."
  [version]
  (merge tests/noop-test 
  	 {:ssh 
  	 	{ :username "root"
  	 	  :private-key-path "~/.ssh/id_rsa"
  	 	}
      :nodeOrder (ref [])
  	 	:nodes     [:n5 :n4 :n1 :n2 :n3  ]
  	   :concurrency 3
  	   :os debian/os
       :db (db version)
  	 }
  	)
 )
