(ns jepsen.cockroach.focus-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.os.debian :as debian]
            [jepsen.cockroach :as cl]
            [jepsen.cockroach [register :as register]]
            [jepsen.cockroach-nemesis :as cln]))

(deftest focus
  (is (:valid?
        (:results
          (jepsen/run!
            (register/test
              {:ssh           nil
               :nodes         [:n1]
               :nemesis       cln/none
               :linearizable  true
               :os            debian/os
               :tarball       "https://binaries.cockroachdb.com/cockroach-beta-20160721.linux-amd64.tgz"}))))))
