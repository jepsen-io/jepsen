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

            		  ; Ensure node is running
                (try (c/exec :service :influxdb :status)
                     (catch RuntimeException _
                     (info "Starting influxdb...")
                     (c/exec :service :influxdb :start)))

            )


      )


    (teardown! [_ test node]
      (info node "tore down"))))

(defn basic-test
  "A simple test of InfluxDB's safety."
  [version]
  (merge tests/noop-test 
  	 {:ssh 
  	 	{ :username "root"
  	 	  :private-key-path "~/.ssh/id_rsa"
  	 	}
  	   :os debian/os
       :db (db version)
  	 }
  	)
 )
