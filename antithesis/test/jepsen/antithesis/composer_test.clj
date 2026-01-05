(ns jepsen.antithesis.composer-test
  (:refer-clojure :exclude [run!])
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
             [clojure.java.shell :refer [sh]]
             [clojure.tools.logging :refer [info warn]]
             [jepsen [checker :as checker]
                     [client :as client]
                     [generator :as gen]
                     [history :as h]
                     [tests :as tests]]
             [jepsen.antithesis.composer :as composer :refer [run!]]))

(defn op!
  "Shells out to the fifo wrapper to perform a single op."
  []
  (sh "composer/op"))

(defn check!
  "Shells out to the fifo wrapper to perform the final check."
  []
  (sh "composer/check"))

(defn make-test
  "Constructs a new test map which records side effects in :effects, and which
  delivers a promise to :setup. Call ((:await-setup test)) to block until all
  clients have completed setup. Call ((:effects! test)) to read and clear side
  effects. We use this in run!-test."
  []
  (let [setup   (promise)
        effects (atom [])
        ; Returns effects and clears them, as a side effect.
        effects! (fn effects! []
                   (let [ret (volatile! [])]
                     (swap! effects (fn [effects]
                                      (vreset! ret effects)
                                      []))
                     @ret))
        test tests/noop-test
        n    (count (:nodes test))
        ; Waits until effects have received n :setup's
        await-setup (fn await-setup []
                      (loop []
                        (if (= n (count (filter #{:setup} @effects)))
                          :setup
                          (do (Thread/sleep 10)
                              (recur)))))
        client (reify client/Client
                 (open! [this test node] this)
                 (setup! [this test]
                   (info "Setup")
                   (swap! effects conj :setup))
                 (invoke! [this test op]
                   (swap! effects conj [:op op])
                   (assoc op :type :ok))
                 (teardown! [this test]
                   (swap! effects conj :teardown))
                 (close! [this test]))
        main-invokes [{:f :write}
                      {:f :read}]
        final-invokes [{:f :final}]
        gen    (gen/clients
                 (gen/phases (composer/main-gen main-invokes)
                             final-invokes))
        checker (reify checker/Checker
                  (check [this test history opts]
                    (swap! effects conj :check)
                    {:valid? (= [:write :read :final]
                                (->> (h/invokes history)
                                     (map :f)))}))]
    (assoc test
           :name "composer test"
           :client client
           :generator gen
           :checker checker
           :effects effects
           :effects! effects!
           :await-setup await-setup
           :nonserializable-keys [:effects :effects! :await-setup])))

(defn strip-time
  "Strips out the :time field of an [:op op] element of an effects vector."
  [effect]
  (if (and (vector? effect) (= :op (first effect)))
    (update effect 1 dissoc :time)
    effect))

(defn strip-times
  "Strips out :time fields from [:op op] elements of an effects vector."
  [effects]
  (mapv strip-time effects))

(deftest immediate-check-test
  (let [{:keys [await-setup effects!] :as test} (make-test)
        n      (count (:nodes test))
        runner (future (run! test))]
    ; We should do setup, then pause.
    (await-setup)
    (is (= (repeat n :setup) (effects!)))

    ; Immediate check
    (is (= {:exit 0, :err "", :out "checked"} (check!)))
    ; We do to the final op, tear down, then check
    (is (= (concat
             [[:op {:index 0, :process 0, :type :invoke, :f :final, :value nil}]]
             (repeat n :teardown)
             [:check])
           (strip-times (effects!))))
    ; And the test is invalid, because we didn't do both ops
    (let [test' @runner]
      (is (= {:valid? false} (:results test'))))))

(deftest early-check-test
  (let [{:keys [await-setup effects!] :as test} (make-test)
        n      (count (:nodes test))
        runner (future (run! test))]
    ; We should do setup, then pause.
    (await-setup)
    (is (= (repeat n :setup) (effects!)))

    ; Do a single op
    (let [op' (op!)]
      (is (= 0 (:exit op')))
      (is (= {:index 1, :type :ok, :process 0, :f :write, :value nil}
             (dissoc (edn/read-string (:out op')) :time))))
    (is (= [[:op {:index 0, :process 0, :type :invoke, :f :write, :value nil}]]
           (strip-times (effects!))))

    ; Early check
    (is (= {:exit 0, :err "", :out "checked"} (check!)))
    (is (= (concat
             [[:op {:index 2, :process 1, :type :invoke, :f :final, :value nil}]]
             (repeat n :teardown)
             [:check])
           (strip-times (effects!))))
    ; Test is invalid because we didn't finish the main gen
    (let [test' @runner]
      (is (= {:valid? false} (:results test'))))))

(deftest full-test
  (let [{:keys [await-setup effects!] :as test} (make-test)
        n      (count (:nodes test))
        runner (future (run! test))]
    ; We should do setup, then pause.
    (await-setup)
    (is (= (repeat n :setup) (effects!)))

    ; Do both ops
    (let [op' (op!)]
      (is (= 0 (:exit op')))
      (is (= {:index 1, :type :ok, :process 0, :f :write, :value nil}
             (dissoc (edn/read-string (:out op')) :time))))
    (is (= [[:op {:index 0, :process 0, :type :invoke, :f :write, :value nil}]]
           (strip-times (effects!))))

    (let [op' (op!)]
      (is (= 0 (:exit op')))
      (is (= {:index 3, :type :ok, :process 1, :f :read, :value nil}
             (dissoc (edn/read-string (:out op')) :time))))
    (is (= [[:op {:index 2, :process 1, :type :invoke, :f :read, :value nil}]]
           (strip-times (effects!))))

    ; Early check
    (is (= {:exit 0, :err "", :out "checked"} (check!)))
    (is (= (concat
             [[:op {:index 4, :process 2, :type :invoke, :f :final, :value nil}]]
             (repeat n :teardown)
             [:check])
           (strip-times (effects!))))
    ; Test is invalid because we didn't finish the main gen
    (let [test' @runner]
      (is (= {:valid? true} (:results test'))))))
