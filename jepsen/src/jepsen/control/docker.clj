(ns jepsen.control.docker
  "The recommended way is to use SSH to setup and teardown databases. It's
  however sometimes conveniet to be able to setup and teardown the databases
  using `docker exec` and `docker cp` instead, which is what this namespace
  helps you do.

  Use at your own risk, this is an unsupported way of running Jepsen."
  (:require [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [throw+]]
            [jepsen.control.core :as core]
            [jepsen.control :as c]))

(defn resolve-container-id
  "Takes a host, e.g. `localhost:30404`, and resolves the Docker container id
  exposing that port. Due to a bug in Docker
  (https://github.com/moby/moby/pull/40442) this is more difficult than it
  should be."
  [host]
  (if-let [[_address port] (str/split host #":")]
    (let [ps (:out (sh "docker" "ps"))
          cid (-> (sh "awk" (str "/" port "/ { print $1 }") :in ps)
                  :out
                  str/trim-newline)]
      (if (re-matches #"[a-z0-9]{12}" cid)
        cid
        (throw+ {:type ::invalid-container-id, :container-id cid})))
    (throw+ {:type ::invalid-host, :host host})))

(defn exec
  "Execute a shell command on a docker container."
  [container-id {:keys [cmd] :as opts}]
  (apply sh
         "docker" "exec" (c/escape container-id)
         "sh" "-c" cmd
         (if-let [in (:in opts)]
           [:in in]
           [])))

(defn- path->container
  [container-id path]
  (str container-id ":" path))

(defn- unwrap-result
  "Throws when shell returned with nonzero exit status."
  [exc-type {:keys [exit] :as result}]
  (if (zero? exit)
    result
    (throw+
     (assoc result :type exc-type)
     nil ; cause
     "Command exited with non-zero status %d:\nSTDOUT:\n%s\n\nSTDERR:\n%s"
     exit
     (:out result)
     (:err result))))

(defn cp-to
  "Copies files from the host to a container filesystem."
  [container-id local-paths remote-path]
  (doseq [local-path (flatten [local-paths])]
    (->> (sh
          "docker" "cp"
          (c/escape local-path)
          (c/escape (path->container container-id remote-path)))
         (unwrap-result ::copy-failed))))

(defn cp-from
  "Copies files from a container filesystem to the host."
  [container-id remote-paths local-path]
  (doseq [remote-path (flatten [remote-paths])]
    (->> (sh
          "docker" "cp"
          (c/escape (path->container container-id remote-path))
          (c/escape local-path))
         (unwrap-result ::copy-failed))))

(defrecord DockerRemote [container-id]
  core/Remote
  (connect [this conn-spec]
    (assoc this :container-id (resolve-container-id (:host conn-spec))))
  (disconnect! [this]
    (dissoc this :container-id))
  (execute! [this ctx action]
    (exec container-id action))
  (upload! [this ctx local-paths remote-path _opts]
    (cp-to container-id local-paths remote-path))
  (download! [this ctx remote-paths local-path _opts]
    (cp-from container-id remote-paths local-path)))

(def docker
  "A remote that does things via `docker exec` and `docker cp`."
  (->DockerRemote nil))
