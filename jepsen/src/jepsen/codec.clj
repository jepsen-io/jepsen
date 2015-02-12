(ns jepsen.codec
  "Serializes and deserializes objects to/from bytes."
  (:require [clojure.edn :as edn]
            [byte-streams :as b])
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
  [bytes]
  (if (nil? bytes)
    nil
    (let [bytes ^bytes (b/to-byte-array bytes)]
      (if (zero? (alength bytes))
        nil
        (with-open [s (ByteArrayInputStream. bytes)
                    i (InputStreamReader. s)
                    r (PushbackReader. i)]
          (binding [*read-eval* false]
            (edn/read r)))))))
