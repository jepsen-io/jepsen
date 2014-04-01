(ns jepsen.elasticsearch
  (:require [clojurewerkz.elastisch.native          :as esn]
            [clojurewerkz.elastisch.native.document :as esd]
            [clojurewerkz.elastisch.native.index    :as esi]
            [jepsen.load :refer [ok]])
  (:use jepsen.set-app)
  (:import [org.elasticsearch.indices IndexMissingException]))



(def index "jepsen_index")
(def es-type "set_app")
(def mapping {es-type
              {:properties
               {:elements {:type "integer" :store "true"}}}})
(def base-doc {:elements []})

(defn clean-index! []
  (try
    (esi/delete index)
    (esi/refresh index)
    (catch IndexMissingException e
      )))

(defn ensure-index! []
  (esi/create index :mappings mapping)
  (esi/refresh index))

(defn seed-index! []
  (esd/create index es-type base-doc))

(defn connect! []
  (esn/connect! [["n1" 9300]
                 ["n2" 9300]
                 ["n3" 9300]
                 ["n4" 9300]
                 ["n5" 9300]]))


(def ids (atom []))

(defn elasticsearch-app
  [opts]

  (let [cluster (connect!)
        id      (atom nil)]

    (reify SetApp

      (setup [app]
        (clean-index!)
        (ensure-index!)
        (let [seeded (seed-index!)]
          (reset! id (:_id seeded))
          (swap! ids conj (:_id seeded))))

      (add [app element]
          (esi/refresh index)
          (let [resp (esd/get index es-type @id)
              vsn (:_version resp)
              elements (-> resp
                         :_source
                         :elements
                         (conj element))]
            (esd/put index es-type @id {:elements elements} :version vsn)))
      
      (results [app]
        (esi/refresh index)
        (apply
          concat (doall (map #(-> (esd/get index es-type %)
                                :_source :elements) @ids))))

      (teardown [app]
        (clean-index!)))))
