(ns yugabyte.common
  (:require [clojure.tools.logging :refer :all]
            [clj-http.client :as http]
            [jepsen.control :as c]
            ))

(defn start-master!
  [node]
  (info (c/exec (c/lit "if [[ -e /home/yugabyte/master/master.out ]]; then /home/yugabyte/bin/yb-server-ctl.sh master start; fi")))
)

(defn start-tserver!
  [node]
  (info (c/exec (c/lit "/home/yugabyte/bin/yb-server-ctl.sh tserver start")))
)

(defn running-masters
  "Returns a list of nodes where master process is running."
  [nodes]
  (->> nodes
       (pmap (fn [node]
               (try
                 (let [is-running
                           (-> (str "http://" node ":7000/jsonmetricz")
                               (http/get)
                               :status
                               (= 200))]
                   [node is-running])
                 (catch Exception e [node false])
               )))
       (filter second)
       (map first)
  ))
