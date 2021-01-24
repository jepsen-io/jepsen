(ns jecci.interface.db
  (:require [jecci.interface.resolver :as r]))

(def db (r/resolve! "db" "db"))