(ns jepsen.tests.kafka-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]
                     [set :as set]]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.tests.kafka :refer :all]))

(defn deindex
  "Strips :index field off a map, or a collection of maps."
  [coll-or-map]
  (if (map? coll-or-map)
    (dissoc coll-or-map :index)
    (map #(dissoc % :index) coll-or-map)))

(defn o
  "Shorthand op constructor"
  [index process type f value]
  (h/op {:index index, :time index, :process process, :type type, :f f,
         :value value}))

(deftest op->max-offsets-test
  (is (= {:x 5 :y 3}
         (op->max-offsets {:type :ok,
                           :f :txn,
                           :value [[:poll {:x [[2 nil] [5 nil] [4 nil]]}]
                                   [:send :y [2 nil]]
                                   [:send :y [3 nil]]]}))))

(deftest log->last-index->values-test
  (testing "empty"
    (is (= [] (log->last-index->values []))))
  (testing "standard"
    (is (= [nil #{:a :b} nil #{:c} #{:d}]
           (log->last-index->values
             [nil #{:a} #{:a :b :c} nil #{:c} #{:c :d} #{:d}])))))

(deftest log->value->first-index-test
  (testing "empty"
    (is (= {} (log->value->first-index []))))
  (testing "standard"
    (is (= {:a 0, :b 1, :c 1, :d 3}
           (log->value->first-index
             [nil #{:a} #{:a :b :c} nil #{:c} #{:c :d} #{:d}])))))

(deftest version-orders-test
  ; Playing a little fast and loose here: when there's conflicts at an offset
  ; we choose a single value nondeterministically.
  (is (= {:orders {:x {:by-index   [:a :c :b :d]
                       :by-value   {:a 0, :b 2, :c 1, :d 3}
                       ; The raw log has a gap at offset 2.
                       :log        [#{:a} #{:b :c} nil #{:b} #{:d}]}}
          ; The write of c at 1 conflicts with the read of b at 1
          :errors [{:key :x, :index 1, :offset 1, :values #{:b :c}}]}
         (version-orders
           ; Read [a b] at offset 0 and 1
           [{:type :ok, :f :txn, :value [[:poll {:x [[0 :a] [1 :b]]}]]}
            ; But write c at offset 1, b at offset 3, and d at offset 4. Even
            ; though this crashes, we can prove it committed because we read
            ; :b.
            {:type :info, :f :txn, :value [[:send :x [1 :c]]
                                           [:send :x [3 :b]]
                                           [:send :x [4 :d]]]}]
           {:ok {:x #{:a :b}}})))

  (testing "a real-world example"
    (let [h [{:type :invoke, :f :send, :value [[:send 11 641]], :time 280153467070, :process 379}
{:type :ok, :f :send, :value [[:send 11 [537 641]]], :time 280169754615, :process 379}
{:type :invoke, :f :send, :value [[:send 11 645]], :time 283654729962, :process 363}
{:type :ok, :f :send, :value [[:send 11 [537 645]]], :time 287474569112, :process 363}
             ]]
      (is (= [{:key 11
               :index  0
               :offset 537
               :values #{641 645}}]
             (:errors (version-orders h {})))))))

(deftest inconsistent-offsets-test
  (testing "info conflicts"
    ; In this example, we have an info send which conflicts with an ok send. We
    ; shouldn't pick this up as a conflict, because we can assume the info
    ; didn't commit.
    (let [send-1  (o 0 0 :invoke :send [[:send :x 1] [:send :y 1]])
          send-1' (o 1 0 :info   :send [[:send :x [0 1]] [:send :y 1]])
          ; This send of 2 conflicts at offset 0
          send-2  (o 2 1 :invoke :send [[:send :x 2]])
          send-2' (o 3 1 :ok     :send [[:send :x [0 2]]])]
      (is (= nil
             (-> [send-1 send-1' send-2 send-2']
                 h/history analysis :errors :inconsistent-offsets)))
      ; But if we introduce a read which observes send-1's send to y, then we
      ; know that either that read was an aborted read, OR that send-1
      ; committed. We assume send-1 committed, and flag this as an
      ; inconsistency.
      (let [poll-1  (o 4 2 :invoke :poll [[:poll]])
            poll-1' (o 5 2 :ok     :poll [[:poll {:y [[5 1]]}]])]
        (is (= [{:key   :x
                 :index  0
                 :offset 0
                 :values #{1 2}}]
               (-> [send-1 send-1' send-2 send-2' poll-1 poll-1']
                   h/history analysis :errors :inconsistent-offsets)))))))



(deftest g1a-test
  ; If we can observe a failed write, we have a case of G1a.
  (let [send  (o 0 0 :invoke :send [[:send :x 2] [:send :y 3]])
        send' (o 1 0 :fail   :send [[:send :x 2] [:send :y 3]])
        poll  (o 2 1 :invoke :poll [[:poll]])
        poll' (o 3 1 :ok     :poll [[:poll {:x [[0 2]]}]])]
    (is (= [{:reader poll'
             :writer send'
             :key   :x
             :value 2}]
           (-> [send send' poll poll'] h/history analysis :errors :G1a)))))

(deftest lost-write-test
  (testing "consistent"
    ; We submit a at offset 0, b at offset 1, and d at offset 3. A read observes
    ; c at offset 2, which implies we should also have read a and b.
    (let [send-a   (o 0 0 :invoke :send [[:send :x :a]])
          send-a'  (o 1 0 :ok     :send [[:send :x [0 :a]]])
          send-bd  (o 2 0 :invoke :send [[:send :x :b] [:send :x :d]])
          send-bd' (o 3 0 :ok     :send [[:send :x [1 :b]] [:send :x [3 :d]]])
          send-c   (o 4 1 :invoke :send [[:send :x :c]])
          send-c'  (o 5 1 :info   :send [[:send :x :c]])
          poll     (o 6 0 :invoke :poll [[:poll]])
          poll'    (o 7 0 :ok     :poll [[:poll {:x [[2 :c]]}]])]
      (is (= [{:key             :x
               :value           :a
               :index           0
               :max-read-index  2
               :writer          send-a'
               :max-read        poll'}
              {:key             :x
               :value           :b
               :index           1
               :max-read-index  2
               :writer          send-bd'
               :max-read        poll'}]
             (-> [send-a send-a' send-bd send-bd' send-c send-c' poll poll']
                 h/history analysis :errors :lost-write)))))

  (testing "inconsistent"
    ; Here, we have inconsistent offsets. a is submitted at offset 0, but gets
    ; overwritten by b at offset 0. c appears at offset 2. We read c, which
    ; means we *also* should have read a and b; however, b's offset could win
    ; when we compute the version order. To compensate, we need more than the
    ; final version order indexes.
    (let [send-a   (o 0 0 :invoke :send [[:send :x :a]])
          send-a'  (o 1 0 :ok     :send [[:send :x [0 :a]]])
          send-bc  (o 2 0 :invoke :send [[:send :x :b] [:send :x :c]])
          send-bc' (o 3 0 :ok     :send [[:send :x [0 :b]] [:send :x [2 :c]]])
          read-bc  (o 4 0 :invoke :poll [[:poll]])
          read-bc' (o 5 0 :ok     :poll [[:poll {:x [[0 :b] [2 :c]]}]])]
      (is (= [{:key             :x
               :value           :a
               :index           0
               :max-read-index  1 ; There is no offset 1
               :writer          send-a'
               :max-read        read-bc'}]
             (-> [send-a send-a' send-bc send-bc' read-bc read-bc']
                 h/history analysis :errors :lost-write)))))

  (testing "atomic"
    ; When we have a crashed transaction, a read of any of its values should
    ; mean that *all* of its values are eligible for lost-update checking. Note
    ; that this relies on still getting offsets out of :info transactions.
    (let [; This send operation crashes, so we normally wouldn't detect it as
          ; a lost update
          send-ab  (o 0 0 :invoke :send [[:send :x :a] [:send :y :b]])
          send-ab' (o 1 0 :info   :send [[:send :x :a] [:send :y [0 :b]]])
          send-c   (o 2 1 :invoke :send [[:send :y :c]])
          send-c'  (o 3 1 :info   :send [[:send :y :c]])
          ; However, this poll tells us send-ab must have committed (or else
          ; we'd have aborted read!
          poll-a   (o 4 2 :invoke :poll [[:poll]])
          poll-a'  (o 5 2 :ok     :poll [[:poll {:x [[0 :a]]}]])
          ; And this tells us that we *should* have read b, since we saw c at a
          ; higher offset
          poll-c   (o 6 3 :invoke :poll [[:poll]])
          poll-c'  (o 7 3 :ok     :poll [[:poll {:y [[1 :c]]}]])]
      ; Without the poll of a, we can't prove send-ab completed, and this is
      ; *not* a lost update.
      (is (= nil
             (-> [send-ab send-ab' send-c send-c' poll-c poll-c']
                 h/history analysis :errors :lost-write)))
      ; But with the poll of a, it *is* a lost update
      (is (= [{:key              :y
               :value            :b
               :index            0
               :max-read-index   1
               :writer           send-ab'
               :max-read         poll-c'}]
             (-> [send-ab send-ab' send-c send-c' poll-a poll-a' poll-c poll-c']
                 h/history analysis :errors :lost-write))))))

(deftest poll-skip-test
  ; Process 0 observes offsets 1, 2, then 4, then 7, but we know 3 and 6
  ; existed due to other reads/writes. 5 might actually be a gap in the log.
  (let [poll-1-2  (o 0 0 :invoke :poll [[:poll]])
        poll-1-2' (o 1 0 :ok     :poll [[:poll {:x [[1 :a], [2 :b]]}]])
        poll-3    (o 2 1 :invoke :poll [[:poll]])
        poll-3'   (o 3 1 :ok     :poll [[:poll {:x [[3 :c]]}]])
        poll-4    (o 6 0 :invoke :poll [[:poll]])
        poll-4'   (o 7 0 :ok     :poll [[:poll {:x [[4 :d]]}]])
        ; Reads and writes that let us know offsets 6 and 7 existed
        write-6   (o 10 2 :invoke :send [[:send :x :f]])
        write-6'  (o 11 2 :ok     :send [[:send :x [6 :f]]])
        poll-7    (o 12 0 :invoke :poll [[:poll]])
        poll-7'   (o 13 0 :ok     :poll [[:poll {:x [[7 :g]]}]])
        ; Sends to fill in polls
        write-*   (o 14 3 :invoke :send [[:send :x :a]
                                         [:send :x :b]
                                         [:send :x :c]
                                         [:send :x :d]
                                         [:send :x :g]])
        write-*'  (o 15 3 :info :send [[:send :x :a]
                                       [:send :x :b]
                                       [:send :x :c]
                                       [:send :x :d]
                                       [:send :x :g]])
        errs [{:key :x
               :ops (deindex [poll-1-2' poll-4'])
               :delta 2
               :skipped [:c]}
              {:key :x
               :ops (deindex [poll-4' poll-7'])
               :delta 2
               :skipped [:f]}]
        nm (fn [history]
             (when-let [es (-> history h/history analysis :errors :poll-skip)]
               ; Strip off indices to simplify test cases
               (map (fn [e] (update e :ops deindex)) es)))]
    (is (= errs (nm [poll-1-2 poll-1-2' poll-3 poll-3' poll-4 poll-4' write-6
                     write-6' poll-7 poll-7' write-* write-*'])))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow skips.
    (testing "with intermediate subscribe"
      (let [sub-xy     (o 4 0 :invoke :subscribe [:x :y])
            sub-xy'    (o 5 0 :ok     :subscribe [:x :y])
            assign-xy  (o 8 0 :invoke :assign    [:x :y])
            assign-xy' (o 9 0 :ok     :assign    [:x :y])
            sub-y      (o 4 0 :invoke :subscribe [:y])
            sub-y'     (o 5 0 :ok     :subscribe [:y])
            assign-y   (o 8 0 :invoke :assign    [:y])
            assign-y'  (o 9 0 :info   :assign    [:y])]
        (is (nil? (nm [poll-1-2 poll-1-2' poll-3 poll-3' sub-y sub-y' poll-4
                       poll-4' assign-y assign-y' write-6 write-6' poll-7
                       poll-7' write-* write-*'])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [poll-1-2 poll-1-2' poll-3 poll-3' sub-xy sub-xy'
                         poll-4 poll-4' assign-xy assign-xy' write-6 write-6'
                         poll-7 poll-7' write-* write-*'])))))))

(deftest nonmonotonic-poll-test
  ; A nonmonotonic poll occurs when a single process performs two transactions,
  ; t1 and t2, both of which poll key k, and t2 begins with a value from k
  ; *prior* to t1's final value.
  ;
  ; Here process 0 polls 1 2 3, then goes back and reads 2 ... again.
  (let [send*     (o 0 0 :invoke :send [[:send :x :a]
                                        [:send :x :b]
                                        [:send :x :c]
                                        [:send :x :d]])
        send*'    (o 1 0 :ok     :send [[:send :x :a]
                                        [:send :x :b]
                                        [:send :x :c]
                                        [:send :x :d]])
        poll-123  (o 2 0 :invoke :poll [[:poll]])
        poll-123' (o 3 0 :ok     :poll [[:poll {:x [[1 :a], [2 :b], [3 :c]]}]])
        poll-234  (o 6 0 :invoke :poll [[:poll]])
        poll-234' (o 7 0 :ok     :poll [[:poll {:x [[2 :b], [3 :c], [4 :d]]}]])
        nm (fn [history]
             (when-let [es (-> history h/history analysis :errors
                               :nonmonotonic-poll)]
               ; Strip off indices to simplify test cases
               (map (fn [e] (update e :ops deindex)) es)))
        errs [{:key    :x
               :ops    (deindex [poll-123' poll-234'])
               :values [:c :b]
               :delta  -1}]]
    (testing "together"
      (is (= errs (nm [send* send*' poll-123 poll-123' poll-234 poll-234']))))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow nonmonotonicity.
    (testing "with intermediate subscribe"
      (let [sub-xy     (o 4 0 :invoke :subscribe [:x :y])
            sub-xy'    (o 5 0 :ok     :subscribe [:x :y])
            assign-xy  (o 4 0 :invoke :assign [:x :y])
            assign-xy' (o 5 0 :ok     :assign [:x :y])
            sub-y      (o 4 0 :invoke :subscribe [:y])
            sub-y'     (o 5 0 :ok     :subscribe [:y])
            assign-y   (o 4 0 :invoke :assign [:y])
            assign-y'  (o 5 0 :ok     :assign [:y])]
        (is (nil? (nm [send* send*' poll-123 poll-123' sub-y sub-y' poll-234
                       poll-234'])))
        (is (nil? (nm [send* send*' poll-123 poll-123' assign-y assign-y'
                       poll-234 poll-234'])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [send* send*' poll-123 poll-123' sub-xy sub-xy'
                         poll-234 poll-234'])))
        (is (= errs (nm [send* send*' poll-123 poll-123' assign-xy assign-xy'
                         poll-234 poll-234'])))))))

(deftest nonmonotonic-send-test
  ; A nonmonotonic send occurs when a single process performs two transactions
  ; t1 and t2, both of which send to key k, and t1's first send winds up
  ; ordered at or before t2's last send in the log.
  ;
  ; Here process 0 sends offsets 3, 4, then sends 1, 2
  (let [send-34  (o 0  0 :invoke :send [[:send :x :c] [:send :x :d]])
        send-34' (o 1  0 :ok     :send [[:send :x [3 :c]] [:send :x [4 :d]]])
        send-12  (o 10 0 :invoke :send [[:send :x 1] [:send :x 2]])
        send-12' (o 11 0 :ok     :send [[:send :x [1 :a]] [:send :x [2 :b]]])
        errs [{:key    :x
               :values [:d :a]
               :delta  -3
               :ops    (deindex [send-34' send-12'])}]
        nm (fn [history]
             (when-let [es (-> history h/history analysis :errors
                               :nonmonotonic-send)]
               ; Strip off indices to simplify test cases
               (map (fn [e] (update e :ops deindex)) es)))]
    (is (= errs (nm [send-34 send-34' send-12 send-12'])))

    ; But if process 0 subscribes/assigns to a set of keys that *doesn't*
    ; include :x, we allow nonmonotonicity.
    (testing "with intermediate subscribe"
      (let [sub-xy     (o 2 0 :invoke :subscribe [:x :y])
            sub-xy'    (o 3 0 :ok     :subscribe [:x :y])
            assign-xy  (o 2 0 :invoke :assign [:x :y])
            assign-xy' (o 3 0 :ok     :assign [:x :y])
            sub-y      (o 2 0 :invoke :subscribe [:y])
            sub-y'     (o 3 0 :ok     :subscribe [:y])
            assign-y   (o 2 0 :invoke :assign [:y])
            assign-y'  (o 3 0 :info   :assign [:y])]
        (is (nil? (nm [send-34 send-34' sub-y sub-y' send-12 send-12'])))
        (is (nil? (nm [send-34 send-34' assign-y assign-y' send-12 send-12'])))
        ; But subscribes that still cover x, we preserve state
        (is (= errs (nm [send-34 send-34' sub-xy sub-xy' send-12 send-12'])))
        (is (= errs (nm [send-34 send-34' assign-xy assign-xy' send-12 send-12'])))))))

(deftest int-poll-skip-test
  ; An *internal poll skip* occurs when within the scope of a single
  ; transaction successive calls to poll() (or a single poll()) skip over a
  ; message we know exists.
  ;
  ; One op observes offsets 1 and 4, but another observes offset 2, which tells
  ; us a gap exists.
  (let [; Sends so we can poll
        send*      (o 0 1 :invoke :send [[:send :x :a]
                                         [:send :x :b]
                                         [:send :x :d]])
        send*'     (o 1 1 :info   :send [[:send :x :a]
                                         [:send :x :b]
                                         [:send :x :d]])
        ; Skip within a poll
        poll-1-4a  (o 2 0 :invoke :poll [[:poll]])
        poll-1-4a' (o 3 0 :ok     :poll [[:poll {:x [[1 :a], [4 :d]]}]])
        ; Skip between polls
        poll-1-4b  (o 4 0 :invoke :poll [[:poll] [:poll]])
        poll-1-4b' (o 5 0 :ok     :poll [[:poll {:x [[1 :a]]}]
                                         [:poll {:x [[4 :d]]}]])
        poll-2     (o 6 0 :invoke :poll [[:poll]])
        poll-2'    (o 7 0 :ok     :poll [[:poll {:x [[2 :b]]}]])]
    (is (= [{:key :x
             :values  [:a :d]
             :skipped [:b]
             :delta 2
             :op poll-1-4a'}
            {:key :x
             :values  [:a :d]
             :skipped [:b]
             :delta 2
             :op poll-1-4b'}]
           (-> [send* send*' poll-1-4a poll-1-4a' poll-1-4b poll-1-4b' poll-2
                poll-2']
               h/history
               analysis
               :errors
               :int-poll-skip)))))

(deftest int-send-skip-test
  ; An *internal send skip* occurs when within the scope of a single
  ; transaction successive calls to send() wind up inserting to offsets which
  ; have other offsets between them.
  ;
  ; Here a single op inserts mixed in with another. We know a's offset, but we
  ; don't know c's. A poll, however, tells us there exists a b between them,
  ; and that c's offset is 3.
  (let [send-13  (o 0 0 :invoke :send [[:send :x :a] [:send :x :c]])
        send-13' (o 1 0 :ok     :send [[:send :x [1 :a]] [:send :x :c]])
        send-b   (o 2 1 :invoke :send [[:send :x :b]])
        send-b'  (o 3 1 :info   :send [[:send :x :b]])
        poll-23  (o 4 2 :invoke :poll [[:poll]])
        poll-23' (o 5 2 :ok     :poll [[:poll {:x [[2 :b] [3 :c]]}]])]
    (is (= [{:key     :x
             :values  [:a :c]
             :skipped [:b]
             :delta   2
             :op      send-13'}]
           (-> [send-13 send-13' send-b send-b' poll-23 poll-23']
               h/history
               analysis
               :errors
               :int-send-skip)))))

(deftest int-nonmonotonic-poll-test
  ; An *internal nonmonotonic poll* occurs within the scope of a single
  ; transaction, where one or more poll() calls yield a pair of values such
  ; that the former has an equal or higher offset than the latter.
  (let [send*     (o 0 0 :invoke :send [[:send :x :a]
                                        [:send :x :b]
                                        [:send :x :c]])
        send*'    (o 1 0 :info   :send [[:send :x :a]
                                        [:send :x :b]
                                        [:send :x :c]])
        poll-31a  (o 2 0 :invoke :poll [[:poll]])
        poll-31a' (o 3 0 :ok     :poll [[:poll {:x [[3 :c] [1 :a]]}]])
        ; This read of :b tells us there was an index between :a and :c; the
        ; delta is therefore -2.
        poll-33b  (o 4 0 :invoke :poll [[:poll]])
        poll-33b' (o 5 0 :ok     :poll [[:poll {:x [[2 :b] [3 :c]]}]
                                        [:poll {:x [[3 :c]]}]])]
    (is (= [{:key    :x
             :values [:c :a]
             :delta  -2
             :op poll-31a'}
            {:key    :x
             :values [:c :c]
             :delta  0
             :op     poll-33b'}]
           (-> [send* send*' poll-31a poll-31a' poll-33b poll-33b']
               h/history
               analysis :errors :int-nonmonotonic-poll)))))

(deftest int-nonmonotonic-send-test
  ; An *internal nonmonotonic send* occurs within the scope of a single
  ; transaction, where two calls to send() insert values in an order which
  ; contradicts the version order.
  (let [; In this case, the offsets are directly out of order.
        send-31a  (o 0 0 :invoke :send [[:send :x 3] [:send :x 1]])
        send-31a' (o 1 0 :ok     :send [[:send :x [3 :c]] [:send :x [1 :a]]])
        ; Or we can infer the order contradiction from poll offsets
        send-42b  (o 2 0 :invoke :send [[:send :y :d] [:send :y :b]])
        send-42b' (o 3 0 :info   :send [[:send :y :d] [:send :y :b]])
        ; c has to come from somewhere, too
        send-3c   (o 4 1 :invoke :send [[:send :y :c]])
        send-3c'  (o 5 1 :info   :send [[:send :y :c]])
        ; This poll tells us b < d on y
        poll-42b  (o 6 0 :invoke :poll [[:poll]])
        poll-42b' (o 7 0 :ok     :poll [[:poll {:y [[2 :b] [3 :c] [4 :d]]}]])]
    (is (= [{:key    :x
             :values [:c :a]
             :delta  -1
             :op     send-31a'}
            {:key    :y
             :values [:d :b]
             :delta  -2
             :op     send-42b'}]
           (-> [send-31a send-31a' send-42b send-42b' send-3c send-3c'
                poll-42b poll-42b']
               h/history
               analysis :errors :int-nonmonotonic-send)))))

(deftest duplicate-test
  ; A duplicate here means that a single value winds up at multiple positions
  ; in the log--reading the same log offset multiple times is a nonmonotonic
  ; poll.
  (let [; Here we have a send operation which puts a to 1, and a poll which
        ; reads a at 3; it must have been at both.
        send-a1  (o 0 0 :invoke :send [[:send :x :a]])
        send-a1' (o 1 0 :ok     :send [[:send :x [1 :a]]])
        send-b2  (o 2 1 :invoke :send [[:send :x :b]])
        send-b2' (o 3 1 :info   :send [[:send :x :b]])
        poll-a3  (o 4 2 :invoke :poll [[:poll]])
        poll-a3' (o 5 2 :ok     :poll [[:poll {:x [[2 :b] [3 :a]]}]])]
    (is (= [{:key   :x
             :value :a
             :count 2}]
           (-> [send-a1 send-a1' send-b2 send-b2' poll-a3 poll-a3'] h/history analysis :errors :duplicate)))))

(deftest realtime-lag-test
  (testing "up to date"
    (let [o (fn [time process type f value]
              {:time time, :process process, :type type, :f f, :value value})
          l (fn [time process k lag]
              {:time time, :process process, :key k, :lag lag})

          history
          (h/history
            [(o 0 0 :invoke :assign [:x])
             (o 1 0 :ok     :assign [:x])
             ; This initial poll should observe nothing
             (o 2 0 :invoke :poll [[:poll]])
             (o 3 0 :ok     :poll [[:poll {:x []}]])
             (o 4 0 :invoke :send [[:send :x :a]])
             (o 5 0 :ok     :send [[:send :x [0 :a]]])
             ; This read started 1 second after x was acked, and failed to
             ; see it; lag must be at least 1.
             (o 6 0 :invoke :poll [[:poll]])
             (o 7 0 :ok     :poll [[:poll {:x []}]])
             (o 8 1 :invoke :send [[:send :x :c] [:send :x :d]])
             (o 9 1 :ok     :send [[:send :x [2 :c]] [:send :x [3 :d]]])
             ; Now we know offsets 1 (empty), 2 (c), and 3 (d) are
             ; present. If we read x=empty again, it must still be from
             ; time 5; the lag is therefor 5.
             (o 10 0 :invoke :poll [[:poll]])
             (o 11 0 :ok     :poll [[:poll]])
             ; Let's read up to [1 :b], which hasn't completed yet, but
             ; which we know was no longer the most recent value as soon
             ; as [2 c] was written, at time 9. Now our lag is 12-9=3.
             (o 12 0 :invoke :poll [[:poll]])
             (o 13 0 :ok     :poll [[:poll {:x [[0 :a] [1 :b]]}]])
             ; If we re-assign process 0 to x, and read nothing, our most
             ; recent read is still of [1 b]; our lag on x is now 16-9=7.
             ; Our lag on y is 0, since nothing was written to y.
             (o 14 0 :invoke :assign [:x :y])
             (o 15 0 :ok     :assign [:x :y])
             (o 16 0 :invoke :poll [[:poll]])
             (o 17 0 :ok     :poll [[:poll {}]])
             ; Now let's assign 0 to y, then x, which clears our offset of
             ; x. If we poll nothing, then we're rewound back to time 5.
             ; Our lag is therefore 22 - 5 = 17.
             (o 18 0 :invoke :assign [:y])
             (o 19 0 :ok     :assign [:y])
             (o 20 0 :invoke :assign [:x])
             (o 21 0 :ok     :assign [:x])
             (o 22 0 :invoke :poll [[:poll]])
             (o 23 0 :ok     :poll [[:poll {}]])
             ; Now let's catch up to the most recent x: [3 d]. Our lag is
             ; 0.
             (o 24 0 :invoke :poll [[:poll] [:poll]])
             (o 25 0 :ok     :poll [[:poll {:x [[0 :a] [1 :b]]}]
                                    [:poll {:x [[2 :c] [3 :d]]}]])
             ; And write x b, which gets reordered to offset 1.
             (o 26 1 :invoke :send [[:send :x :b]])
             (o 27 1 :info   :send [[:send :x :b]])
             ])]
      (testing realtime-lag
        (is (= [(l 2 0 :x 0)
                (l 6 0 :x 1)
                (l 10 0 :x 5)
                (l 12 0 :x 3)
                (l 16 0 :x 7) (l 16 0 :y 0)
                (l 22 0 :x 17)
                (l 24 0 :x 0)]
               (realtime-lag history))))
      (testing "worst realtime lag"
        (is (= {:time 22, :process 0, :key :x, :lag 17}
               (:worst-realtime-lag (analysis history))))))))

(deftest empty-history-test
  (let [history (h/history [(o 0 0 :invoke :assign [:x])
                            (o 1 0 :ok :assign [:x])])
        c (checker)
        check (checker/check c
                             {:name "empty-history-test"
                              :start-time 0
                              :history history}
                             history
                             nil)]
    (is (not (:valid? check)))))

(deftest unseen-test
  (is (= [{:time 1, :unseen {}}
          {:time 3, :unseen {:x 2}}
          {:time 5, :unseen {:x 1}}
          {:time 7, :unseen {:x 0}, :messages {:x #{}}}]
         (-> [(o 0 0 :invoke :poll [[:poll]])
              (o 1 0 :ok     :poll [[:poll {}]])
              (o 2 0 :invoke :send [[:send :x :a] [:send :x :b]])
              (o 3 0 :ok     :send [[:send :x [0 :a]] [:send :x [1 :b]]])
              (o 4 0 :invoke :poll [[:poll]])
              (o 5 0 :ok     :poll [[:poll {:x [[0 :a]]}]])
              (o 6 0 :invoke :poll [[:poll]])
              (o 7 0 :ok     :poll [[:poll {:x [[1 :b]]}]])
              ]
             h/history
             analysis
             :unseen))))

(deftest g0-test
  ; Here, two transactions write an object to two different keys, and obtain
  ; conflicting orders: a write cycle.
  (let [wa  (o 0 0 :invoke :send [[:send :x :a] [:send :y :a]])
        wb  (o 1 1 :invoke :send [[:send :x :b] [:send :y :b]])
        wa' (o 2 0 :ok     :send [[:send :x [0 :a]] [:send :y [1 :a]]])
        wb' (o 3 1 :ok     :send [[:send :x [1 :b]] [:send :y [0 :b]]])]
    (is (= [{:type  :G0
             :cycle [wb' wa' wb']
             :steps [{:type :ww, :key :y, :value :b, :value' :a
                      :a-mop-index 1, :b-mop-index 1}
                     {:type :ww, :key :x, :value :a, :value' :b,
                      :a-mop-index 0, :b-mop-index 0}]}]
           (-> [wa wb wa' wb'] h/history
               (analysis {:ww-deps true}) :errors :G0)))))

(deftest g1c-pure-wr-test
  ; Transaction t1 is visible to t2, and t2 is visible to t1
  (let [t1  (o 0 0 :invoke :txn [[:send :x :a] [:poll]])
        t2  (o 1 1 :invoke :txn [[:send :y :b] [:poll]])
        t1' (o 2 0 :ok :txn [[:send :x [0 :a]] [:poll {:y [[0 :b]]}]])
        t2' (o 3 1 :ok :txn [[:send :y [0 :b]] [:poll {:x [[0 :a]]}]])]
    (is (= [{:type :G1c
             :cycle [t2' t1' t2']
             :steps [{:type :wr, :key :y, :value :b,
                      :a-mop-index 0, :b-mop-index 1}
                     {:type :wr, :key :x, :value :a
                      :a-mop-index 0, :b-mop-index 1}]}]
           (-> [t1 t2 t1' t2'] h/history analysis :errors :G1c)))))

(deftest g1c-ww-wr-test
  ; Transaction t1 writes something which is followed by a write of t2, but
  ; also observes a different write from t2
  (let [t1  (o 0 0 :invoke :txn [[:send :x :a]          [:send :y :c]])
        t2  (o 1 1 :invoke :txn [[:poll]                [:send :y :b]])
        t1' (o 2 0 :ok     :txn [[:send :x [0 :a]]      [:send :y [2 :c]]])
        t2' (o 3 1 :ok     :txn [[:poll {:x [[0 :a]]}]  [:send :y [1 :b]]])]
    (is (= [{:type :G1c
             :cycle [t1' t2' t1']
             :steps [{:type :wr, :key :x, :value :a,
                      :a-mop-index 0, :b-mop-index 0}
                     {:type :ww, :key :y, :value :b, :value' :c,
                      :a-mop-index 1, :b-mop-index 1}]}]
           (-> [t1 t2 t1' t2'] h/history
               (analysis {:ww-deps true}) :errors :G1c)))))
