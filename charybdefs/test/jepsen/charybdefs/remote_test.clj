(ns jepsen.charybdefs.remote-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.charybdefs :as charybdefs]
            [config.core :refer [env]]))

;;; To run these tests, create a file config.edn in ../.. (the 'test' directory)
;;; containing at least:
;;;   {:hostname "foo"}
;;; This must name a host you can ssh to without a password, and have passwordless
;;; sudo on. It must run debian or ubuntu (tested with ubuntu 16.04).
;;; Other ssh options may also be used.
(use-fixtures :each
  (fn [f]
    (if (not (:hostname env))
      (throw (RuntimeException. "hostname is required in config.edn")))
    (c/with-ssh (select-keys env [:private-key-path :strict-host-key-checking :username])
      (c/on (:hostname env)
            (charybdefs/install!)
            (f)))))

(deftest break-fix
  (testing "break the disk and then fix it"
    (let [filename "/faulty/foo"]
      (c/exec :touch filename)
      (charybdefs/break-all)
      (is (thrown? RuntimeException (c/exec :cat filename)))
      (charybdefs/clear)
      (is "" (c/exec :cat filename)))))
