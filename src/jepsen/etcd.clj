(ns jepsen.etcd
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
        jepsen.load
        )
  (:require [cetcd.core :as etcd]
            [clojure.string :as string]
            [jepsen.failure :as failure]
            [jepsen.control.net :as control.net]
            [jepsen.control :as control]))


(defn etcd-app
  (let [directory "/jepsen/"]
    (reify SetApp
           (setup [app]
                  (teardown app))
           (add [app element]
                (etcd/set-key! (string/join directory element)
                               element))
           (results [app]
                    (set (map #(:value %)
                              (-> (etcd/get-key directory) :node :nodes))))
           (teardown [app]
                     (etcd/delete-key! directory true)))))
