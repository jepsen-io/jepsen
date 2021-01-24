(ns jecci.interface.resolver
  (:require [jecci.settings :as s]
            [clojure.tools.logging :refer [info warn error]]
            ))


(defn resolve!
  [submodule sym]
  (try
    (var-get (requiring-resolve (symbol (str "jecci." s/system "." submodule "/" sym))))
    (catch Exception e
      (warn "Cannot resolve " sym)
      nil)))

(defmacro resolve-macro!
  [submodule sym & body]
  `(~(requiring-resolve (symbol (str "jecci." s/system "." submodule "/" sym))) ~@body))
