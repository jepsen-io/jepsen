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
      (try+ (read-block-index h1)
            (is false)
            (catch [:type :jepsen.store.format/block-header-truncated] e
              (is (= 0 (:length e))))))

    (testing "trivial index"
      (write-header! h1)
      (write-block-index! h1)
      (flush! h1)
      (is (= {(int 1) 10} (read-block-index h2))))))

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
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "writing block 2"
      ; Prepare
      (write-header! h1)
      (write-block-index! h1)
      ; Build block
      (let [offset (next-block-offset h1)
            id     (new-block-id! h1)
            data   {:foo 2 :bar ["cat" #{'mew}]}]
        (is (= offset 230))
        (is (= id 2))
        ; Write block
        (write-fressian-block! h1 offset data)
        (assoc-block! h1 id offset)
        (write-block-index! h1)
        (flush! h1)

        ; Now... can we read it?
        (refresh-block-index! h2)
        (is (= data (read-fressian-block h2 id)))
        ))))

(deftest partial-map-test
  (let [shallow {:shallow "waters"}
        deep    {:deep "lore"}]
    (testing "write"
      (with-open [h (open file)]
        (write-header! h)
        (write-block-index! h)
        (let [id1     (new-block-id! h)
              id2     (new-block-id! h)
              offset2 (next-block-offset h)]
          (is (= id1 (int 2)))
          ; Write block 2 first
          (write-partial-map-block! h offset2 deep nil)
          (assoc-block! h id2 offset2)
          ; Then block 1, pointing to 2
          (let [offset1 (next-block-offset h)]
            (write-partial-map-block! h offset1 shallow id2)
            (assoc-block! h id1 offset1))
          ; And finalize
          (write-block-index! h)
          (flush! h))))

    (testing "read"
      (with-open [h (open file)]
        (refresh-block-index! h)
        (let [m (read-partial-map-block h (int 2))]
          (is (instance? jepsen.store.format.PartialMap m))
          (is (= "waters" (:shallow m)))
          (is (= "lore" (:deep m)))
          (is (= (merge deep shallow) m)))))))
