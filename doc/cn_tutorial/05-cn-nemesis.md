# 故障引入

nemesis是一个不绑定到任何特定节点的特殊客户端，用于引入整个集群内运行过程中可能遇到的故障。我们需要导入`jepsen.nemesis`来提供数个内置的故障模式。

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

我们将选取一个简单的nemesis进行介绍，并在测试中添加名为`:nemesis`的主键。当它收到`:start`操作指令时，它会将网络分成两部分并随机选择其中一个。当收到`:stop`指令时则恢复网络分区。

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

像常规的客户端一样，nemesis从生成器中获取操作。现在我们的生成器会将操作分发给常规的客户端——而nemesis只会收到`nil`，即什么都不用做。我们将专门用于nemesis操作的生成器来替换它。我们也准备增加时间限制，那样就有足够的时间等着nemesis发挥作用了。

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

Clojure sequence数据结构可以扮演生成器的角色，因此我们可以使用Clojure自带的函数来构建它们。这里，我们使用`cycle`来构建一个无限的睡眠、启动、睡眠、停止循环，直至超时。

网络分区造成一些操作出现崩溃：

```clojure
WARN [2018-02-02 15:54:53,380] jepsen worker 1 - jepsen.core Process 1 crashed
java.net.SocketTimeoutException: Read timed out
```

如果我们*知道*一个操作没有触发，我们可以通过返回带有`:type :fail`代替`client/invoke!`抛出异常让checker更有效率（也能发现更多的bugs！），但每个错误引发程序崩溃依旧是安全的：jepsen的checkers知道一个已经崩溃的操作可能触发也可能没触发。

# 发现bug

我们已经在测试中写死了超时时间为30s，但是如果能够在命令行中控制它就好了。Jepsen的cli工具箱提供了一个`--time-limit`开关，在参数列表中，它作为`:time-limit`传给`etcd-test`。现在我们把它的使用方法展示出来。

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

现在我们的测试时间可长可短，让我们加速请求访问速率。如果两次请求时间间隔太长，那么我们就看不到一些有趣的行为。我们将两次请求的时间间隔设置为1/10s。

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

如果你多次运行这个测试，你会注意到一个有趣的结果。有些时候它会失败！

```bash
$ lein run test --test-count 10
...
     :model {:msg "can't read 3 from register 4"}}]
...
Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```

Knossos参数有误：它认为寄存器需要的合法参数个数是4，但是程序成功读取到的是3。当出现线性验证失败时，Knossos将绘制一个SVG图展示错误——我们可以读取历史记录来查看更详细的操作信息。

```bash
$ open store/latest/linear.svg
$ open store/latest/history.txt
```

这是读取操作常见的脏读问题：尽管最近得一些写操作已完成了，我们依然获取了一个`过去`值。这种情况出现是因为etcd允许我们读取任何副本的局部状态，而不需要经过共识特性来确保我们拥有最新的状态。

# 线性一致读

etcd文档宣称"默认情况下etcd确保所有的操作都是线性一致性"，但是显然事实并非如此，在[第二版api文档](https://coreos.com/etcd/docs/latest/v2/api.html)隐藏着这么一条不引人注意的注释：
> 如果你想让一次读取是完全的线性一致，可以使用quorum=true。读取和写入的操作路径会因而变得非常相似，并且具有相近的速度（译者注：暗指速率变慢）。如果不确定是否需要此功能，请随时向etcd开发者发送电子邮件以获取建议。

啊哈！所以我们需要使用*quorum*读取，Verschlimmbesserung中有这样的案例：

```clojure
    (invoke! [this test op]
      (case (:f op)
        :read (let [value (-> conn
                              (v/get "foo" {:quorum? true})
                              parse-long)]
                (assoc op :type :ok, :value value))
      ...
```

引入quorum读取后测试通过。

```clojure
$ lein run test
...
Everything looks good! ヽ(‘ー`)ノ
```

恭喜！你已经成功写完了第一个Jepsen测试，我在[2014](https://aphyr.com/posts/316-jepsen-etcd-and-consul)年提出了这个issue，并且联系了etcd开发团队请他们介绍*quorum*读机制。

休息一下吧，这是你应得的！如果你喜欢的话，可以继续[完善Jepsen测试](https://github.com/jaydenwen123/jepsen/blob/main/doc/cn_tutorial/06-cn-refining.md)