(ns jepsen.pg
  (:require [clojure.string :as string])
  (:use [clojure.set :only [union difference]]
        [korma.core :exclude [union]]
        [korma.db :only [postgres create-db with-db transaction]]
        [jepsen.control.net :only [hosts-map]]
        jepsen.set-app)
  )

(defn connect [host]
  (create-db {:subprotocol "postgresql"
              :subname (str "//" host "/jepsen?"
                            "loginTimeout=1&"
                            "socketTimeout=1&"
                            "tcpKeepAlive=true")
              :user "jepsen"
              :password "jepsen"}))

(def db-lock (Object.))

(defn pg-app
  [opts]
  (let [table   (get opts :coll "set_app")
        db      (connect (:n1 jepsen.control.net/hosts-map))]

    (reify SetApp
      (setup [app]
             (locking db-lock
               (teardown app)
               (with-db db
                        (exec-raw "CREATE TABLE set_app (id serial primary key, 
                                   element int not null)")
                        (assert (-> table
                                  (select (aggregate (count :*) :count))
                                  first
                                  :count
                                  zero?)))))

      (add [app element]
           (with-db db
                    (transaction
                      (insert table (values {:element element})))))

      (results [app]
               (map :element
                    (with-db db
                             (select table))))

      (teardown [app]
                (with-db db
                         (exec-raw "DROP TABLE IF EXISTS set_app CASCADE"))))))
