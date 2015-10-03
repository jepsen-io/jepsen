(ns jepsen.zookeeper.leaderd
  (:gen-class)
  (:require [clojure.java.io :as io]
            [aleph.http :as http]
            [cheshire.core :as json])
  (:import (org.apache.curator.utils CloseableUtils)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.retry ExponentialBackoffRetry)
           (org.apache.curator.framework.recipes.leader
             LeaderSelectorListenerAdapter
             LeaderSelector)
           (java.io Closeable
                    IOException
                    BufferedReader
                    InputStreamReader)))

(def path "/jepsen/leaderd")

(defn framework
  [connect-string]
  (doto (CuratorFrameworkFactory/newClient connect-string
                                           (ExponentialBackoffRetry. 1000 3))
    (.start)))

(defn leader!
  "Takes a function to invoke while leadership is held."
  [framework path f]
  (let [leader-selector (promise)
        listener  (proxy [LeaderSelectorListenerAdapter] []
                    (close []
                      (.close @leader-selector))

                    (takeLeadership [framework]
                      (f)))]
    (deliver leader-selector (LeaderSelector. framework path listener))

    ; Automatically re-queue when leadership complete.
    (.autoRequeue @leader-selector)

    (.start @leader-selector)
    @leader-selector))

(defmacro while-leader
  "Repeats body while a leader in ZK."
  [framework path & body]
  `(leader! ~framework ~path (bound-fn [] ~@body)))

(defn http-server!
  "Takes a leader state atom to serve over HTTP port 5678"
  [state]
  (http/start-server (fn [req]
                       {:status 200
                        :headers {"content-type" "application/json"}
                        :body    (json/generate-string @state)})
                     {:port 5678}))

(defn -main
  [connect-string]
  (let [framework (framework connect-string)
        state     (atom {:leader false
                         :epoch  0})]

    (while-leader framework path
                  (try
                    (swap! state (fn [state]
                                   (assoc state
                                          :leader true
                                          :epoch (inc (:epoch state)))))
                    (prn @state)
                    (Thread/sleep 10000000)
                    (catch InterruptedException e
                      (prn "Interrupted!")
                      (.. Thread currentThread interrupt))
                    (finally
                      (swap! state assoc :leader false)
                      (prn @state))))

    ; Sleep now, sweet prince
    (while true
      (Thread/sleep 10000))))
