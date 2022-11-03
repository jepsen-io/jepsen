(ns jepsen.checker.timeline-test
  (:refer-clojure :exclude [set])
  (:require [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [checker :refer :all]
                    [history :as h]
                    [store :as store]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.checker.timeline :as t]))

(deftest timeline-test
  (let [test (assoc tests/noop-test
                    :start-time 0)
        history (h/history
                  [{:process 0, :time 0, :type :invoke, :f :write, :value 3}
                   {:process 1, :time 1000000, :type :invoke, :f :read, :value nil}
                   {:process 0, :time 2000000, :type :info, :f :read, :value nil}
                   {:process 1, :time 3000000, :type :ok, :f :read, :value 3}])
        opts {}]
  (is (= [:html
           [:head
            [:style
             ".ops        { position: absolute; }\n.op         { position: absolute; padding: 2px; border-radius: 2px; box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); transition: all 0.3s cubic-bezier(.25,.8,.25,1); overflow: hidden; }\n.op.invoke  { background: #eeeeee; }\n.op.ok      { background: #6DB6FE; }\n.op.info    { background: #FFAA26; }\n.op.fail    { background: #FEB5DA; }\n.op:target  { box-shadow: 0 14px 28px rgba(0,0,0,0.25), 0 10px 10px rgba(0,0,0,0.22); }\n"]]
           [:body
            [:div
             [:a {:href "/"} "jepsen"]
             " / "
             [:a {:href "/files/noop"} "noop"]
             " / "
             [:a {:href "/files/noop/0"} "0"]
             " / "
             [:a {:href "/files/noop/0/"} "independent"]
             " / "
             [:a {:href "/files/noop/0/independent/"} ""]]
            [:h1 "noop key "]
            nil
            [:div
             {:class "ops"}
             [[:a
               {:href "#i2"}
               [:div
                {:class "op info",
                 :id "i2",
                 :style "width:100;left:0;top:0;height:80",
                 :title
                 "Dur: 2 ms\nErr: nil\nWall-clock Time: 1970-01-01T00:00:00.002Z\n\nOp:\n{:process 0\n :type :info\n :f :read\n :index 2\n :value }"}
                "0 read 3<br />"]]
              [:a
               {:href "#i3"}
               [:div
                {:class "op ok",
                 :id "i3",
                 :style "width:100;left:106;top:16;height:32",
                 :title
                 "Dur: 2 ms\nErr: nil\nWall-clock Time: 1970-01-01T00:00:00.003Z\n\nOp:\n{:process 1\n :type :ok\n :f :read\n :index 3\n :value 3}"}
                "1 read <br />3"]]]]]]
         (t/hiccup test history opts)))))
