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
Clojure序列可以扮演生成器的角色,因此我们可以使用Clojure自带的函数来构建它们.这里,我们使用`cycle`来构建一个无限的sleep循环,开始,sleep,停止,直至超时.  
网络分区造成一些操作出现crash:
```clojure
WARN [2018-02-02 15:54:53,380] jepsen worker 1 - jepsen.core Process 1 crashed
java.net.SocketTimeoutException: Read timed out
```
等等... 如果我们*know*一个操作没有触发,我们可以通过返回带有`:type :fail`代替`client/invoke!`抛出异常让checker更有效率(也能发现更多的bugs!),但每个错误导致程序crash依旧是安全的:jepsen的checkers知道一个已经crash的操作是否可能触发
# 发现bug
我们已经在测试中写死了超时时间30s,但是如果能够在命令行中控制它就好了.Jepsen的cli工具箱提供了一个`--time-limit`开关,在参数列表中,它作为`:time-limit`传给`etcd-test`.现在我们把它的使用方法挂出来.
```clojure
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
```
```bash
$ lein run test --time-limit 60
...
```
现在我们的测试可长可短,让我们加速请求访问速率.如果两次请求时间间隔太长,那么我们就看不到一些有趣的行为.我们将两次请求的时间间隔设置为1/10s
```clojure
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1/50)
                          (gen/nemesis
                           (cycle [(gen/sleep 5)
                            {:type :info, :f :start}
                            (gen/sleep 5)
                            {:type :info, :f :stop}]))
                          (gen/time-limit (:time-limit opts)))
```
如果你多次运行这个测试,你会注意到一个有趣的结果.有些时候它会失败.
```bash
$ lein run test --test-count 10
...
     :model {:msg "can't read 3 from register 4"}}]
...
Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```
Knossos参数有误:它认为合法的参数个数是4,但是程序成功读取到的数量是3.当出现线性失败时,Knossos将回执一个SVG图展示错误--我们可以读取历史记录来查看更详细的操作信息.
```bash
$ open store/latest/linear.svg
$ open store/latest/history.txt
```
# 线性一致读
etcd文档宣称"默认情况下etcd确保所有的操作都是线性一致性",但是显然事实并非如此,在[第二版api文档](https://coreos.com/etcd/docs/latest/v2/api.html)隐藏着这么一条不引人注意的注释:
> 如果你想线性一致读,可以使用quorm=true.读取和写入的路径非常相似,并且具有相似的速度.如果不确定是否需要此功能,请随时向etcd开发者发送电子邮件以获取建议

Aha!所以我们需要使用*quorum*读取,教程中有这样的案例:
```clojure
    (invoke! [this test op]
      (case (:f op)
        :read (let [value (-> conn
                              (v/get "foo" {:quorum? true})
                              parse-long)]
                (assoc op :type :ok, :value value))
      ...
```
引入quorum读取后测试通过
```clojure
$ lein run test
...
Everything looks good! ヽ(‘ー`)ノ
```
恭喜!你已经成功写了第一个Jepsen测试,我在[2014](https://aphyr.com/posts/316-jepsen-etcd-and-consul)年提出了这个issue,并且联系了etcd开发团队请他们介绍*quorum*读机制
休息一下吧,你太棒了!如果你喜欢的话,可以移步[提炼测试](https://github.com/jaydenwen123/jepsen/blob/main/doc/cn_tutorial/06-cn-refining.md)