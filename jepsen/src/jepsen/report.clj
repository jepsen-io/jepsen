(ns jepsen.report
  "Prints out stuff."
  (:require [jepsen.util :as util]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]))

(defmacro to
  "Binds stdout to a file for the duration of body."
  [filename & body]
  `(let [filename# ~filename]
    (io/make-parents filename#)
    (with-open [w# (io/writer filename#)]
      (try
        (binding [*out* w#] ~@body)
        (finally
          (println "Report written to" filename#))))))
