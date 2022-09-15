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
                (try (io/delete-file file)
                     (catch java.io.IOException _
                       ; Likely doesn't exist
                      ))
                (t)))

(defmacro is-thrown+
  "Evals body and asserts that the slingshot pattern is thrown."
  [pattern & body]
  `(try+ ~@body
         (is false)
         (catch ~pattern e#
           (is true))))

(deftest header-test
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "empty file"
      (is-thrown+ [:type     :jepsen.store.format/magic-mismatch
                   :expected "JEPSEN"
                   :actual   :eof]
                  (check-magic h2))

      (is-thrown+ [:type :jepsen.store.format/version-mismatch]
                  (check-version h2)))

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
      (is-thrown+ [:type :jepsen.store.format/no-block-index]
                  (load-block-index! h1)))

    (testing "trivial index"
      (write-block-index! h1)
      (load-block-index! h2)
      (is (= {:root nil
              :blocks {(int 1) 18}}
             @(:block-index h2))))))

(deftest read-block-by-id-test
  (with-open [h1 (open file)
              h2 (open file)]
    (testing "empty file"
      (is-thrown+ [:type            :jepsen.store.format/block-not-found
                   :id              (int 1)
                   :known-block-ids []]
                  (read-block-by-id h1 (int 1))))))

(deftest fressian-block-test
  (with-open [h1 (open file)
              h2 (open file)]
    (let [data {:foo 2 :bar ["cat" #{'mew}]}]
      (testing "writing"
        (->> (write-fressian-block! h1 data)
             (set-root! h1)
             write-block-index!))

      (testing "reading"
        (is (= data (:data (read-root h2))))))))

(deftest partial-map-test
  (let [shallow {:shallow "waters"}
        deep    {:deep "lore"}]
    (testing "write"
      (with-open [h (open file)]
        ; Just to push things around a bit
        (write-block-index! h)
        (let [; Write deep block
              deep-id    (write-partial-map-block! h deep nil)
              ; Then shallow block pointing to deep
              shallow-id (write-partial-map-block! h shallow deep-id)]
          ; Make it the root
          (set-root! h shallow-id))

        ; And finalize
        (write-block-index! h)))

    (testing "read"
      (with-open [h (open file)]
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
      (write-test! h test))

    (with-open [h (open file)]
      (let [test' (read-test h)]
        (is (= test test'))))))

;; Test-specific tests
(let [base-test  {:name  "a test"
                  :state :base}
      history    [{:type :invoke} {:type :ok}]
      run-test   (assoc base-test
                        :state  :run
                        :history history)
      results    {:valid? false, :vent-cores :frog-blasted}
      final-test (assoc run-test
                        :state   :final
                        :results results)
      read-test #(read-test (open file))]
  (deftest crash-recovery-test
    (let [history-id (promise)]
      (with-open [w (open file)]
        (testing "empty file"
          (is-thrown+ [:type :jepsen.store.format/magic-mismatch]
                      (read-test)))

        (testing "just header"
          (write-header! w)
          (is-thrown+ [:type :jepsen.store.format/no-block-index]
                      (read-test)))

        (testing "base test"
          (let [id (write-fressian-block! w base-test)]
            (is-thrown+ [:type :jepsen.store.format/no-block-index]
                        (read-test))

            (set-root! w id)
            (write-block-index! w)
            (is (= base-test (read-test)))))

        (testing "history"
          ; Write history; test should be unchanged.
          (deliver history-id (write-fressian-block! w history))
          (is (= base-test (read-test)))

          ; Write new test referencing that history; test still unchanged.
          (let [test-id (write-fressian-block!
                          w (assoc run-test :history (block-ref @history-id)))]
            (is (= base-test (read-test)))

            ; Once we write the block index with the new root, test should
            ; reflect the history.
            (-> w (set-root! test-id) write-block-index!)
            (is (= run-test (read-test)))))

        (testing "analysis"
          ; Write more results
          (let [more-results-id (write-partial-map-block!
                                  w (dissoc results :valid?) nil)
                _ (is (= run-test (read-test)))

                ; Write main results
                results-id (write-partial-map-block!
                             w (select-keys results [:valid?]) more-results-id)
                _ (is (= run-test (read-test)))

                ; And new test block
                test-id (write-fressian-block!
                          w (assoc final-test
                                   :history (block-ref @history-id)
                                   :results (block-ref results-id)))]
            (is (= run-test (read-test)))

            ; Save!
            (-> w (set-root! test-id) write-block-index!)
            (is (= final-test (read-test))))))))

  (deftest incremental-test-write-test
    (is-thrown+ [:type :jepsen.store.format/magic-mismatch] (read-test))

    ; Write initial test
    (with-open [w (open file)]
      (write-initial-test! w base-test)
      (is (= base-test (read-test)))

      ; Write history
      (let [run-test' (write-history! w run-test)]
        (is (= run-test (read-test)))

        ; Write results
        (let [final-test' (merge run-test' final-test)]
          (is-thrown+ [:type :jepsen.store.format/no-history-id-in-meta]
                      (write-results! w final-test))
          (write-results! w final-test')
          (is (= final-test (read-test)))

          ; Write NEW results on top of old ones
          (let [final-test' (assoc final-test' :results {:valid?   :unknown
                                                         :whoopsie :daisy})]
            (write-results! w final-test')
            (is (= final-test' (read-test)))

            ; GC that!
            (let [size1 (.size ^java.nio.channels.FileChannel (:file w))
                  _     (close! w)
                  _     (gc! file)
                  size2 (with-open [r (open file)] (.size ^java.nio.channels.FileChannel (:file r)))]
              (is (= final-test' (read-test)))
              (is (= 1128 size1))
              (is (= 383 size2))

              ; Now we should be able to open up this test, update its
              ; analysis, and write it back *re-using* the existing history.
              (with-open [r (open file)
                          w (open file)]
                (let [test  (jepsen.store.format/read-test r)
                      test' (assoc test :results {:valid? :rewritten
                                                  :new    :findings})]
                  (write-results! w test')
                  (is (= test' (read-test))))))))))))
