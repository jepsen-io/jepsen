(ns jepsen.store.format-test
  (:require [byte-streams :as bs]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.store.format :refer :all]
            [jepsen.store :as store]
            [slingshot.slingshot :refer [try+ throw+]]))

(def file "/tmp/jepsen-format-test.jepsen")

(use-fixtures :each
              (fn wipe-file [t]
                (io/delete-file file)
                (t)))

(deftest header-test
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "empty file"
      (try+ (check-magic h2)
            (is false)
            (catch [:type :jepsen.store.format/magic-mismatch] e
              (is (= {:type     :jepsen.store.format/magic-mismatch
                      :expected "JEPSEN"
                      :actual   :eof}
                     e))))
      (try+ (check-version h2)
            (is false)
            (catch [:type :jepsen.store.format/version-mismatch] e
              (is (= {:type     :jepsen.store.format/version-mismatch
                      :expected 0
                      :actual   :eof}
                     e)))))

    ; Write header
    (write-header! h1)
    (flush! h1)

    (testing "with header"
      (is (= h2 (check-magic h2)))
      (is (= h2 (check-version h2))))))

(deftest block-index-test
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "empty file"
      (try+ (load-block-index! h1)
            (is false)
            (catch [:type :jepsen.store.format/no-root] e
              (is true))))

    (testing "trivial index"
      (write-header! h1)
      (write-block-index! h1)
      (flush! h1)
      (load-block-index! h2)
      (is (= {:root nil
              :blocks {(int 1) 18}}
             @(:block-index h2))))))

(deftest read-block-by-id-test
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "empty file"
      (try+ (read-block-by-id h1 (int 1))
            (is false)
            (catch [:type :jepsen.store.format/block-not-found] e
              (is (= (int 1) (:id e)))
              (is (= [] (:known-block-ids e))))))))

(deftest fressian-block-test
  (with-open [h1   (open file)
              h2   (open file)]
    (let [data {:foo 2 :bar ["cat" #{'mew}]}]
      (testing "writing block 2"
        ; Write header and block, and make it the root
        (write-header! h1)
        (->> (write-fressian-block! h1 data)
             (set-root! h1)
             write-block-index!
             flush!)

        ; Now... can we read it?
        (load-block-index! h2)
        (is (= data (:data (read-root h2))))))))

(deftest partial-map-test
  (let [shallow {:shallow "waters"}
        deep    {:deep "lore"}]
    (testing "write"
      (with-open [h (open file)]
        (write-header! h)
        (write-block-index! h)
        (let [; Write deep block
              deep-id    (write-partial-map-block! h deep nil)
              ; Then shallow block pointing to deep
              shallow-id (write-partial-map-block! h shallow deep-id)]
          ; Make it the root
          (set-root! h shallow-id))

        ; And finalize
        (write-block-index! h)
        (flush! h)))

    (testing "read"
      (with-open [h (open file)]
        (load-block-index! h)
        (let [m (:data (read-root h))]
          ; Instance checks get awkward with hot code reloading
          (is (= "jepsen.store.format.PartialMap" (.getName (class m))))
          (is (= "waters" (:shallow m)))
          (is (= "lore" (:deep m)))
          (is (= (merge deep shallow) m)))))))

(deftest write-test-test
  (let [test {:history [:a :b :c]
              :results {:valid? false
                        :more [:oh :no]}
              :name "hello"
              :generator [:complex]}]
    (with-open [h (open file)]
      (write-header! h)
      (write-test! h test))

    (with-open [h (open file)]
      (load-block-index! h)
      (let [test' (read-test h)]
        (is (= test test'))))))
