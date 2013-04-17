(ns jepsen.core
  (:use lamina.core
        aleph.tcp))

(defrecord Server
  [source dest acceptor clients state])

(defn client
  "Opens a channel to the given destination address."
  [dest]
  (wait-for-result
    (tcp-client dest)))

(defn handle
  "Accepts new connections for a server. Called with a Server, lamina channel,
  and information about the client."
  [server ch client-info]
  (println "New connection to" server "from" client-info)
  ; Connect to upstream server.
  (let [client (client (:dest server))]
    (println "Client to" (:dest server) "connected")

    ; Mark this client as active.
    (swap! (:clients server) conj client)
    
    ; Logging connection closes.
    (on-closed client (fn []
                        (println "Upstream closed.")))
    (on-closed ch     (fn []
                        (println "Downstream closed.")
                        (swap! (:clients server) disj client)))

    (let [partitioner (fn [_] (= :connected (deref (:state server))))]
      ; Connect channels.
      (siphon
        (filter* partitioner client) 
        ch)
      (siphon
        (filter* partitioner ch)
        client))

      (println "Connection established.")))

(defn stop!
  "Shuts down a server, closing all connections."
  [server]
  (locking server
    (when-let [acceptor (deref (:acceptor server))]
      (acceptor)))
  server)

(defn start!
  "Starts up a server."
  [server]
  (locking server
    (stop! server)
    (reset! (:state server) :connected)
    (reset! (:clients server) #{})
    (reset! (:acceptor server)
           (start-tcp-server
             (partial handle server)
             (:source server))))
  server)

(defn server
  "Creates a new server which listens on source and proxies to dest. Doesn't
  start anything."
  [source dest]
  (Server. source dest (atom nil) (atom nil) (atom nil)))
  
(defn server!
  "Starts a proxy server which binds to source and forwards connections to
  dest. Returns the server."
  [source dest]
  (start! (server source dest)))

(defn kick!
  "Closes all active connections through a server."
  [server]
  (dorun (map close (deref (:clients server)))))

(defn partition!
  "Partitions a server: drops all messages on the floor."
  [server]
  (reset! (:state server) :partitioned))

(defn unpartition!
  "Resolves a network partition."
  [server]
  (kick! server)
  (reset! (:state server) :connected))
