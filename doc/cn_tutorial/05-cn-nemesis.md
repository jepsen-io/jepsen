# 错误详解
nemesis是一个特殊的客户端,没有绑定到任何特定的节点,它展示了贯穿整个集群中的错误.我们需要使用`jepsen.nemesis`提供几个模型内嵌的错误
```clojure
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```
我们将选取一个简单的nemesis进行介绍,并在测试中添加`:nemesis`.当它收到`:start`操作指令时,它会将网络分成两部分并随机选择其中一部分;当收到`:stop`指令时则恢复网络分区
```clojure
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "etcd"
          :os              debian/os
          :db              (db "v3.1.5")
          :client          (Client. nil)
          :nemesis         (nemesis/partition-random-halves)
          :checker         (checker/compose
                             {:perf   (checker/perf)
                              :linear (checker/linearizable
                                        {:model     (model/cas-register)
                                         :algorithm :linear})
                              :timeline (timeline/html)})
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```
像常规的客户端一样,nemesis从生成器中执行操作.现在我们的生成器会将操作分发给常规的客户端--如果nemesis收到`nil`,则表示什么都不用做.我们将专门用于nemesis操作的生成器替换它.我们也准备增加时间限制,那样就有足够的时间等着nemesis发挥作用了
```clojure
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (cycle [(gen/sleep 5)
                              {:type :info, :f :start}
                              (gen/sleep 5)
                              {:type :info, :f :stop}]))
                          (gen/time-limit 30))
```
