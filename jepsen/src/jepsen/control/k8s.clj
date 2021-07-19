(ns jepsen.control.k8s
  "The recommended way is to use SSH to setup and teardown databases.
  It's however sometimes conveniet to be able to setup and teardown
  the databases using `kubectl` instead, which is what this namespace
  helps you do. Use at your own risk, this is an unsupported way
  of running Jepsen."
  (:require [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [throw+]]
            [jepsen.control.core :as core]
            [jepsen.control :as c]
            [clojure.string :refer [split-lines trim]]
            [clojure.tools.logging :refer [info]]))

(defn exec
  "Execute a shell command on a pod."
  [context namespace pod-name {:keys [cmd] :as opts}]
  (apply sh
         "kubectl"
         "exec"
         (c/escape pod-name)
         context
         namespace
         "--"
         "sh"
         "-c"
         cmd
         (if-let [in (:in opts)]
           [:in in]
           [])))

(defn- path->pod
  [pod-name path]
  (str pod-name ":" path))

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
  "Copies files from the host to a pod filesystem."
  [context namespace pod-name local-paths remote-path]
  (doseq [local-path (flatten [local-paths])]
    (->> (sh
          "kubectl"
          "cp"
          (c/escape local-path)
          (c/escape (path->pod pod-name remote-path))
          context
          namespace)
         (unwrap-result ::copy-failed))))

(defn cp-from
  "Copies files from a pod filesystem to the host."
  [context namespace pod-name remote-paths local-path]
  (doseq [remote-path (flatten [remote-paths])]
    (->> (sh
          "kubectl"
          "cp"
          (c/escape (path->pod pod-name remote-path))
          (c/escape local-path)
          context
          namespace)
         (unwrap-result ::copy-failed))))

(defn- or-parameter
  "A helper function that encodes a parameter if present"
  [p, v]
  (if v (str "--" p "=" (c/escape v)) ""))

(defrecord K8sRemote [context namespace]
  core/Remote
  (connect [this conn-spec]
    (assoc this
           :context   (or-parameter "context" context)
           :namespace (or-parameter "namespace" namespace)
           :pod-name  (:host conn-spec)))
  (disconnect! [this]
    (dissoc this :context :namespace :pod-name))
  (execute! [this ctx action]
    (exec context namespace (:pod-name this) action))
  (upload! [this ctx local-paths remote-path _opts]
    (cp-to context namespace (:pod-name this) local-paths remote-path))
  (download! [this ctx remote-paths local-path _opts]
    (cp-from context namespace (:pod-name this) remote-paths local-path)))

(defn k8s
  "Returns a remote that does things via `kubectl exec` and `kubectl cp`, in the default context and namespacd."
  []
  (->K8sRemote nil nil))

(defn list-pods
  "A helper function to list all pods in a given context/namespace"
  [context namespace]
  (let [context (or-parameter "context" context)
        namespace (or-parameter "namespace" namespace)
        res (sh
             "sh"
             "-c"
             (str "kubectl get pods " context " " namespace
                  " | tail -n +2 "
                  " | awk '{print $1}'"))]
    (lazy-seq (split-lines (trim (:out res))))))
