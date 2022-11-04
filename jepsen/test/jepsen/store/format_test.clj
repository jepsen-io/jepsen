(ns jepsen.store.format-test
  (:require [byte-streams :as bs]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.store.format :refer :all]
            [jepsen [history :as h]
                    [store :as store]]
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

      (is-thrown+ [:type :jepsen.store.format/version-incomplete]
                  (check-version! h2)))

    ; Write header
    (write-header! h1)
    (flush! h1)

    (testing "with header"
      (is (= h2 (check-magic h2)))
      (is (= h2 (check-version! h2)))
      (is (= current-version (version h2))))))

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
  (with-open [w (open file)]
    (testing "empty file"
      (with-open [r (open file)]
        (is-thrown+ [:type            :jepsen.store.format/magic-mismatch
                     :expected        "JEPSEN"
                     :actual          :eof]
                    (read-block-by-id r (int 1)))))

    (testing "file with no blocks"
      (write-block-index! w)
      (with-open [r (open file)]
        (is-thrown+ [:type            :jepsen.store.format/block-not-found
                     :id              (int 4)
                     :known-block-ids [1]]
                    (read-block-by-id r (int 4)))))))

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

(defn write-big-vector!
  "Takes a vector and writes it to the file as a big vector. Returns the block
  ID of the written block."
  [v]
  (with-open [h (open file)]
    ; Just to mess with offsets
    (write-block-index! h)
    (let [w (big-vector-block-writer! h 5)]
      (reduce append-to-big-vector-block! w v)
      (.close w)
      (:block-id w))))

(defn check-big-vector!
  "Writes and reads a bigvector, ensuring it's equal."
  [v]
  (let [id (write-big-vector! v)]
    (with-open [h (open file)]
      (let [v2 (:data (read-block-by-id h id))]
        (testing "type"
          (is (= "jepsen.history.core.SoftChunkedVector" (.getName (type v2)))))

        (testing "equality"
          ;(info :read-bigvec v2)
          ; Count
          (is (= (count v) (count v2)))
          ; Via reduce
          (is (= v (into [] v2)))
          ; Via seq
          (is (= v v2))
          ; Via parallel reducers/fold
          (is (= v (into [] (r/foldcat v2))))
          ; Via nth
          (doseq [i (range (count v))]
            (is (= (nth v i) (nth v2 i)))))

        (testing "hashing"
          (is (= (hash v) (hash v2))))

        (testing "conj"
          (is (= (conj v ::x) (conj v2 ::x))))

        (testing "assoc"
          (when (pos? (count v))
            (is (= (assoc v 0 ::x) (assoc v2 0 ::x)))
            (let [i (dec (count v))]
              (is (= (assoc v i ::x) (assoc v2 i ::x))))))

        (testing "contains?"
          (is (false? (contains? v2 -1)))
          (when (pos? (count v))
            (is (true? (contains? v2 0)))
            (is (true? (contains? v2 (dec (count v))))))
          (is (false? (contains? v2 (count v)))))

        (testing "rseq"
          (is (= (rseq v) (rseq v2))))

        (testing "peek"
          (is (= (peek v) (peek v2))))

        (testing "pop"
          (when (pos? (count v))
            (is (= (pop v) (pop v2)))))

        (testing "empty"
          (let [e (empty v2)]
            ; Right now our implementation falls back to regular vectors for
            ; this
            ;(is (= "jepsen.store.format.BigVector" (.getName (class e))))
            (is (= 0 (count e)))
            (is (= [] e))))

        (testing "subvec"
          (when (pos? (count v))
            (let [i (dec (count v))]
              (testing "after first"
                (is (= (subvec v 1)   (subvec v2 1))))
              (testing "up to last"
                (is (= (subvec v 0 i) (subvec v2 0 i))))
              (when (< 1 (count v))
                (testing "after first, up to last"
                  (is (= (subvec v 1 i) (subvec v2 1 i))))))))
        ))))

(deftest big-vector-test
  (testing "0"
    (check-big-vector! []))
  (testing "1"
    (check-big-vector! [:x]))
  (testing "6"
    (check-big-vector! [:a :b :c :d :e :f]))
  (testing "128"
    (check-big-vector! (vec (range 128)))))

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
                  :state :base
                  :history nil}
      history-size        12
      history-chunk-size  5
      history    (->> (range history-size)
                      (mapcat (fn [i]
                                [{:type :invoke, :process i}
                                 {:type :ok, :process i}]))
                      h/history)
      run-test   (assoc base-test
                        :state   :run
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
          ; First write an empty history block
          (deliver history-id (write-fressian-block! w nil))
          (info :history-id @history-id)

          (let [base-test' (assoc base-test :history (block-ref @history-id))
                id (write-fressian-block! w base-test')]
            (is-thrown+ [:type :jepsen.store.format/no-block-index]
                        (read-test))

            (set-root! w id)
            (write-block-index! w)
            (is (= base-test (read-test)))))

        (testing "history"
          (let [hw (big-vector-block-writer! w @history-id history-chunk-size)
                ; Intermediate indices in the history where we check the state
                first-cut  1
                second-cut (+ history-chunk-size 1)]
            ; Stream the first op into the history. Test should be unchanged.
            (reduce append-to-big-vector-block! hw (subvec history 0 first-cut))
            (is (= base-test (read-test)))

            ; Stream the first block. Test should now have a partial history up
            ; to the first chunk.
            (reduce append-to-big-vector-block! hw
                    (subvec history 1 (inc history-chunk-size)))
            (let [t (loop []
                      ; Writing is async, so we do a little spinloop here
                      (let [t (read-test)]
                        (if (seq (:history t))
                          t
                          (do (Thread/sleep 5)
                              (recur)))))]
              (is (= (assoc base-test :history
                            (subvec history 0 history-chunk-size))
                     t)))

            ; Stream the remainder of the history and close the writer; the
            ; test should now have the full history.
            (reduce append-to-big-vector-block! hw (subvec history second-cut))
            (.close hw)
            (let [t (read-test)]
              (is (= (assoc base-test :history history) t))
              (is (= "jepsen.history.DenseHistory" (.getName (type (:history t)))))
              (is (= "jepsen.history.Op" (.getName (type (first (:history t)))))))))

        (testing "run test"
          ; Write the run test version--it might have different fields than the
          ; original.
          (let [id (write-fressian-block! w (assoc run-test :history
                                                   (block-ref @history-id)))]
            (set-root! w id)
            (write-block-index! w))
          (is (= run-test (read-test))))

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
    ; This checks the write-initial-test, write-results, etc. functions
    (is-thrown+ [:type :jepsen.store.format/magic-mismatch] (read-test))

    ; Write initial test
    (with-open [w (open file)]
      (let [base-test' (write-initial-test! w base-test)]
        (is (= base-test (read-test)))

        ; Write history
        (with-open [hw (test-history-writer! w base-test')]
          (reduce append-to-big-vector-block! hw history))
        (is (= (assoc base-test :history history) (read-test)))

        ; Rewrite test
        (let [run-test' (merge base-test' run-test)
              run-test' (write-test-with-history! w run-test')]
          (is (= run-test (read-test)))

          ; Write results
          (let [final-test' (merge run-test' final-test)]
            (is-thrown+ [:type :jepsen.store.format/no-history-id-in-meta]
                        (write-test-with-results! w final-test))
            (write-test-with-results! w final-test')
            (is (= final-test (read-test)))

            ; Write NEW results on top of old ones
            (let [final-test' (assoc final-test' :results {:valid?   :unknown
                                                           :whoopsie :daisy})]
              (write-test-with-results! w final-test')
              (is (= final-test' (read-test)))

              ; GC that!
              (let [size1 (.size ^java.nio.channels.FileChannel (:file w))
                    _     (close! w)
                    _     (gc! file)
                    size2 (with-open [r (open file)]
                            (.size ^java.nio.channels.FileChannel (:file r)))]
                (is (= final-test' (read-test)))
                (is (= 1602 size1))
                (is (= 639 size2))

                ; Now we should be able to open up this test, update its
                ; analysis, and write it back *re-using* the existing history.
                (with-open [r (open file)
                            w (open file)]
                  (let [test  (jepsen.store.format/read-test r)
                        test' (assoc test :results {:valid? :rewritten
                                                    :new    :findings})]
                    (write-test-with-results! w test')
                    (is (= test' (read-test)))))))))))))
