# 提炼测试


我们的测试确定了一个故障，但需要一些运气和聪明的猜测去偶然发现，现在是时候提炼我们的测试了，去使得它更快、更容易理解以及功能更加强大。

为了分析单个key的历史记录，jepsen通过搜索并发操作的每种排列，以查找遵循cas操作规则的历史记录，这意味着在任何给定的时间点的并发操作数后，我们的搜索是指数级的


jepsen 运行时需要指定一个工作线程数，这通常情况下也限制并发操作的数量。但是，当操作**crash**(或者是返回一个`:info`的结果，再或者是抛出一个异常),我们放弃该操作并且让当前线程去做新的事情。这可能会出现如下情况：crashed的进程操作仍然在运行，并且可能会在后面的时间里被数据库执行。这意味着**对于后面整个历史记录的剩余时间内，crashed的操作是并发的。**


crashed的操作越多，历史记录结束时的并发操作就越多。并发数线性的增加伴随着验证时间的指数增加。我们首要的命令就是减少crashed操作的数量，下面我们将从读取开始

## Crashed reads

当一个操作超时时，我们会得到类似下面的这样一长串的堆栈信息

```
WARN [2018-02-02 16:14:37,588] jepsen worker 1 - jepsen.core Process 11 crashed
java.net.SocketTimeoutException: Read timed out
  at java.net.SocketInputStream.socketRead0(Native Method) ~[na:1.8.0_40]
  ...
```

... 同时进程的操作转成了一个`:info`的消息，因为我们不能确定该操作是成功了还是失败了。
但是，*幂等*操作，向读操作并没有改变系统的状态。读操作是否成功不影响，因为效果是相同的。因此我们可以安全的将crashed读操作转为读操作失败，并提升校验检查的性能。

```clj
  (invoke! [_ test op]
    (case (:f op)
      :read  (try (let [value (-> conn
                                  (v/get "foo" {:quorum? true})
                                  parse-long)]
                    (assoc op :type :ok, :value value))
                  (catch java.net.SocketTimeoutException ex
                    (assoc op :type :fail, :error :timeout)))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type :ok))
      :cas (try+
             (let [[old new] (:value op)]
               (assoc op :type (if (v/cas! conn "foo" old new)
                                 :ok
                                 :fail)))
             (catch [:errorCode 100] ex
               (assoc op :type :fail, :error :not-found)))))
```

更好的是，如果我们一旦能捕获三个路径中的网络超时异常，我们就可以避免所有的异常堆栈信息出现在日志中。我们也将处理不存在的错误(not-found errors)，尽管它只出现在`:cas`操作中，处理该错误后，将能保持代码更加的清爽。



```clj
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (let [value (-> conn
                              (v/get "foo" {:quorum? true})
                              parse-long)]
                (assoc op :type :ok, :value value))
        :write (do (v/reset! conn "foo" (:value op))
                   (assoc op :type :ok))
        :cas (let [[old new] (:value op)]
               (assoc op :type (if (v/cas! conn "foo" old new)
                                 :ok
                                 :fail))))

      (catch java.net.SocketTimeoutException e
        (assoc op
               :type  (if (= :read (:f op)) :fail :info)
               :error :timeout))

      (catch [:errorCode 100] e
        (assoc op :type :fail, :error :not-found))))
```

现在**所有**的操作，我们会得到很短的超时错误信息，不仅仅读操作。

```bash
INFO [2017-03-31 19:34:47,351] jepsen worker 4 - jepsen.util 4  :info :cas  [4 4] :timeout
```

## Independent keys

我们有一个单个key线性一致性的工作测试。但是，迟早进程将会crash，并且并发数将会上升，分析变慢。我们需要一种方法来**绑定**单个key的历史操作记录的长度，同时又能执行足够多的操作来观察并发错误。因为独立的keys的操作线性独立，因此我们**提升**我们的单个key测试为对多个key操作的测试，`jepsen.independent`命名空间提这样的支持。

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```

我们有一个generator，它可以对单个key发出操作，例如：`{:type :invoke,
:f :write, :value 3}`。我们想提升这个操作为写**多个**key。代替`:value v`，我们想`value [key v]`

```clj
          :generator  (->> (independent/concurrent-generator
                             10
                             (range)
                             (fn [k]
                               (->> (gen/mix [r w cas])
                                    (gen/stagger 1/50)
                                    (gen/limit 100))))
                           (gen/nemesis
                             (cycle [(gen/sleep 5)
                                     {:type :info, :f :start}
                                     (gen/sleep 5)
                                     {:type :info, :f :stop}]))
                           (gen/time-limit (:time-limit opts)))}))
```

我们的read、write、cas操作的组合仍然不变，但是它被包裹在一个**函数**内，这个函数有一个参数k并且返回一个指定key的values的generator。我们使用`concurrent-generator`,使得每个key有10个线程，keys来自无限的证书序列`(range)`,同时这些keys的generators生成自`(fn [k] ...)`
`concurrent-generator` 改变了我们的values的结构，从`v`变成了`[k v]`，因此我们需要更新我们的client，以便它知道对不同的keys如何读和写。

```clj
  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (try+
        (case (:f op)
          :read (let [value (-> conn
                                (v/get k {:quorum? true})
                                parse-long)]
                  (assoc op :type :ok, :value (independent/tuple k value)))

          :write (do (v/reset! conn k v)
                     (assoc op :type :ok))

          :cas (let [[old new] v]
                 (assoc op :type (if (v/cas! conn k old new)
                                   :ok
                                   :fail))))

        (catch java.net.SocketTimeoutException e
          (assoc op
                 :type  (if (= :read (:f op)) :fail :info)
                 :error :timeout))

        (catch [:errorCode 100] e
          (assoc op :type :fail, :error :not-found)))))
```

看看我们的硬编码的key `"foo"`是如何消失的？现在每个key都被操作自身参数化了。注意我们更新value的位置--例如：`:f :read`--我们必须构建一个指定 `independent/tuple` 的键值对。具有元组的特殊数据类型，它能允许`jepsen.independent`在后面分隔历史记录。

最后，我们的checker以单个值的方式尽心思考--但是我们可以转换一个checker，它通过由key标识来**independent**的值。

```clj
          :checker   (checker/compose
                       {:perf  (checker/perf)
                        :indep (independent/checker
                                 (checker/compose
                                   {:linear   (checker/linearizable {:model (model/cas-register)
                                                                     :algorithm :linear})
                                    :timeline (timeline/html)}))})
```


写一个checker，免费获得一个由n个checker构成的家庭，哈哈哈哈

```bash
$ lein run test --time-limit 30
...
ERROR [2017-03-31 19:51:28,300] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
java.util.concurrent.ExecutionException: java.lang.AssertionError: Assert failed: This jepsen.independent/concurrent-generator has 5 threads to work with, but can only use 0 of those threads to run 0 concurrent keys with 10 threads apiece. Consider raising or lowering the test's :concurrency to a multiple of 10.
```


阿哈，我们默认的并发是5个线程，但我们为了运行单个key，至少10个，运行10个key的话，需要100个线程。

```bash
$ lein run test --time-limit 30 --concurrency 100
...
142 :invoke :read [134 nil]
67  :invoke :read [133 nil]
66  :ok :read [133 1]
101 :ok :read [137 3]
181 :ok :write  [135 3]
116 :ok :read [131 3]
111 :fail :cas  [131 [0 0]]
151 :invoke :read [138 nil]
129 :ok :write  [130 2]
159 :ok :read [138 1]
64  :ok :write  [133 0]
69  :ok :cas  [133 [0 0]]
109 :ok :cas  [137 [4 3]]
89  :ok :read [135 1]
139 :ok :read [139 4]
19  :fail :cas  [131 [2 1]]
124 :fail :cas  [130 [4 4]]
```

看上述结果，在有限的时间窗口内我们可以执行更多的操作。这帮助我们能能快的发现bugs。

到目前为止，我们硬编码的地方很多，让我们在命令行中进行这些选择[配置](07-cn-parameters.md)
