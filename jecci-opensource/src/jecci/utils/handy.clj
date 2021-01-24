(ns jecci.utils.handy
  "Some handy little functions"
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [slingshot.slingshot :refer [throw+ try+]]
            [jepsen.control :as c]
            [clojure.java.io :as io]))

(defn exec->info
  [res]
  (info res))

(defn exec->warn
    [res]
      (warn res))

(defn exec->error
    [res]
      (error res))

(defn gen-datetime-id
    "generate datetime string, replacing space and : with _"
      []
        (str/replace (str/replace (str (new java.util.Date)) ":" "_") " " "_"))

(defn write-resource-file2nodefile
  [path-in-resource dst-file]
  (c/exec :echo (slurp (io/resource path-in-resource)) :> dst-file))

(defn change-own
  [path user]
  (c/su
    (c/exec "chown" user path)
    (c/exec "chgrp" user path)))

(defn change-permit
  ([path permit]
   (c/su
     (c/exec "chmod" permit path)))
  ([path]
   (change-permit path "750")))

