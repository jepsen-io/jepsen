(ns cockroach.client
  (:require [clj-http.client          :as http]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]))
(defn write!
  ([base k value] (write! base k value nil))
  ([base k value exp_value]
   ; TODO actually implement CAS
   (http/post (str base k) { :body (str value) })))
                     
(defn read!
  [base k]
  (http/get (str base k)))
