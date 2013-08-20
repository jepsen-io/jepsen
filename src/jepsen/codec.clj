(ns jepsen.codec
  "Serializes and deserializes objects to/from bytes."
  (:require [clojure.edn :as edn])
  (:import  (java.io ByteArrayInputStream
                     InputStreamReader
                     PushbackReader)))

(defn encode
  "Serialize an object to bytes."
  [o]
  (if (nil? o)
    (byte-array 0)
    (binding [*print-dup* false]
      (-> o pr-str .getBytes))))

(defn decode
  "Deserialize bytes to an object."
  [^bytes bytes]
  (if (or (nil? bytes) (= 0 (alength bytes)))
    nil
    (with-open [s (ByteArrayInputStream. bytes)
                i (InputStreamReader. s)
                r (PushbackReader. i)]
      (binding [*read-eval* false]
        (edn/read r)))))

