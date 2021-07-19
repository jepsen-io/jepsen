# 添加一个Set测试
我们可以将etcd集群建模为一组寄存器，每个寄存器用一个key来标识，并且该寄存器支持read、write、cas操作。但这不是我们可以建立在etcd之上的唯一可能的系统。例如，我们将其视为一组key，并且忽略其value。或者我们可以实现一个基于etcd集群的队列。理论上，我们可以对etcd API的每个部分进行建模，但是状态空间将会很大，而且实现可能很耗时。典型地，我们将重点介绍API的重要部分或者常用部分。


但是什么情况下一个测试有用呢？我们的线性一致性测试相当笼统，执行不同类型的随机操作，并且决定这些操作的任何模式是否都是线性的。然而，这么做代价也是非常昂贵的，如果我们能设计一个简单验证的测试，这就太好了，但它仍然能告诉我们一些有用的信息



考虑一个支持`add`和`read`操作的集合。如果我们只读，通过观察空集合就能满足我们的测试。如果我们只写，每个测试将总会通过，因为给一个集合中添加元素总是合法的。很明显，我们需要读写结合。此外，一个读操作应该是最后发生的一个，因为最终读操作**之后**的任何写操作将不会影响测试输出


我们应该添加什么元素？如果我们总是添加相同的元素，该测试具有一定的分辨能力：如果每次添加都返回`ok`，但是我们不去读该元素，我们知道我们发现了一个bug。然而，如果任何的添加有效，那么最终的读将会包含该元素，并且我们无法确定其他添加的元素是否有效。或许对元素去重是有用的，这样每个添加操作对该读操作产生一些独立的影响。如果我们选择有序的元素，我们可以粗略的了解损失是随着时间平均分布还是成块出现，因此，我们也打算这样做。

我们的操作将会类似于下面这样

```clj
{:type :invoke, :f :add, :value 0}
{:type :invoke, :f :add, :value 1}
...
{:type :invoke, :f :read, :value #{0 1}}
```

如果每个添加都成功，我们将知道数据库正确的执行了，并存在于最终读的结果中。通过执行多次读操作和追踪哪些读操作完成了或者哪些到目前为止正在进行中，我们能获得更多的信息。但现在，让我们先简单进行

## A New Namespace

在`jepsen.etcdemo`中开始变的有些混乱了，因此我们要将这些内容分解为新测试的专用命名空间中。我们将称为`jepsen.etcdemo.set`:

```sh
$ mkdir src/jepsen/etcdemo
$ vim src/jepsen/etcdemo/set.clj
```

我们将设计一个新的client和generator，因此我们需要下面这些jepsen中的命名空间。当然，我们将使用我们的etcd client库，Verschlimmbesserung--我们将处理来自它的异常，因此也需要Slingshot库

```clj
(ns jepsen.etcdemo.set
  (:require [jepsen
              [checker :as checker]
              [client :as client]
              [generator :as gen]]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```



我们将需要一个能往集合中添加元素，并能读取元素的一个client--但我们必须选择如何在数据库中存储上面的集合set。一个选择是使用独立的key，或者一个key池子。另一个选择是使用单个key，并且其value是一个序列化的数据类型，类似于json数组或者Clojure的set，我们将使用后者。

```clj
(defrecord SetClient [k conn]
  client/Client
    (open! [this test node]
        (assoc this :conn (v/connect (client-url node)
```


Oh。有一个问题。我们没有`client-url`函数。我们可以从`jepsen.etcdemo`提取它，但我们后面想使用`jepsen.etcdemo`的*this*命名空间，并且Clojure非常艰难的尝试避免命名空间中的循环依赖问题。我们创建一个新的称为`jepsen.etcdemo.support`的命名空间。像`jepsen.etcdemo.set`一样，它也会有它自己的文件。

```bash
$ vim src/jepsen/etcdemo/support.clj
```


让我们将url构造函数从`jepsen.etcdemo` 移动到`jepsen.etcdemo.support`

```clj
(ns jepsen.etcdemo.support
  (:require [clojure.string :as str]))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" node ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))
```


现在我们在`jepsen.etcdemo`需要support命名空间，并且替换，用新名称调用这些函数：

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
					  ...
            [jepsen.etcdemo.support :as s]
            ...))

...

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/" version
                       "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :--log-output                   :stderr
          :--name                         node
          :--listen-peer-urls             (s/peer-url   node)
          :--listen-client-urls           (s/client-url node)
          :--advertise-client-urls        (s/client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (s/peer-url node)
          :--initial-cluster              (s/initial-cluster test))

        (Thread/sleep 5000)))

...

    (assoc this :conn (v/connect (s/client-url node)
```


处理完之后，回到`jepsen.etcdemo.set`,这里也需要我们的support命名空间，并且在client中使用它

```clj
(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (s/client-url node)
                                 {:timeout 5000})))
```


我们将使用`setup!`函数来初始化空Clojure set:`#{}`中的单个key的value。我们将再一次硬编码，但在SetClient中有一个字段的话，将会更加清晰一些。

```clj
  (setup! [this test]
    (v/reset! conn k "#{}"))
```


我们的`invoke`函数看起来和之前的client中的实现有一些相似，我们将基于`:f`来分发处理，并使用相似的错误处理器。

```clj
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (assoc op
                     :type :ok
                     :value (read-string
                              (v/get conn k {:quorum? (:quorum test)})))
```

怎么样往集合中添加一个元素呢？我们需要去读取当前集合，添加新value，如果它的值未变的话，然后写入它。Verschlimmbesserung有一个[helper for that](https://github.com/aphyr/verschlimmbesserung)`swap!`函数，它可以转换该key的值

```clj
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (read-string
                              (v/get conn k {:quorum? (:quorum test)})))

        :add (do (v/swap! conn k (fn [value]
                                   (-> value
                                       read-string
                                       (conj (:value op))
                                       pr-str)))
                 (assoc op :type :ok)))

      (catch java.net.SocketTimeoutException e
        (assoc op
               :type  (if (= :read (:f op)) :fail :info)
               :error :timeout))))
```


我们清除我们这儿的key，但是处于该教程的目，我们将跳过这部分，当测试开始的时候，它将会删除所有剩余的数据。


```clj
  (teardown! [_ test])

  (close! [_ test]))
```


Good！现在我们需要用generator和checker来打包。我们会使用相同的名字、OS、DB、来自线性测试中的nemesis，为了代替准备一个*full*的test map，我们将称它为"wordload"，并且将其集成到后面的测试中。


添加一个元素到set中是一个通用的测试,jepsen中内置了一个`checker/set`.

```clj
(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (SetClient. "a-set" nil)
   :checker   (checker/set)
   :generator
```

对于generator... hmm。我们知道它处理两个部分：首先，我们将添加一组元素，并且在完成后，我们将执行单一次读取。让我们现在独立的编写这两部分，并且考虑如何将它们结合。


我们如何获得一组唯一的元素去添加呢？我们可以从头编写一个generator，但是使用Clojure内置的序列库来构建一个调用操作序列，每个数字一次，然后将其包裹在使用`gen/seq`生成额generator中，或许更容易一些，，就像我们为nemesis做的starts，sleeps,stops的无限循环那样。


```clj
(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client (SetClient. "a-set" nil)
   :checker (checker/set)
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
```


对于final-generator，我们使用`gen/once`来发出一次读，而不是无限次的读取

## Integrating the New Workload


现在，我们需要集成workload到主函数的`etcd-test`中，让我们回到`jepsen.etcdemo`，并且require set测试命名空间。

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
						...
            [jepsen.etcdemo [set :as set]
                            [support :as s]]
```

看`etcd-test`,我们可以直接编辑它，但是最终我们将要**回到**我们的线性测试中，因此让我们暂时保留所有内容，并添加一个新的map，基于设置的workload覆盖调client，checker，generator

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key."
  [opts]
  (let [quorum    (boolean (:quorum opts))
        workload  (set/workload opts)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "etcd q=" quorum)
            :quorum          quorum
            :os              debian/os
            :db              (db "v3.1.5")
            :client          (Client. nil)
            :nemesis         (nemesis/partition-random-halves)
            :checker         (checker/compose
                               {:perf   (checker/perf)
                                :indep (independent/checker
                                         (checker/compose
                                           {:linear   (checker/linearizable
                                                        {:model (model/cas-register)
                                                         :algorithm :linear})
                                            :timeline (timeline/html)}))})
            :generator       (->> (independent/concurrent-generator
                                    10
                                    (range)
                                    (fn [k]
                                      (->> (gen/mix [r w cas])
                                           (gen/stagger (/ (:rate opts)))
                                           (gen/limit (:ops-per-key opts)))))
                                  (gen/nemesis
                                    (->> [(gen/sleep 5)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}]
                                         cycle))
                                  (gen/time-limit (:time-limit opts)))}
           {:client    (:client workload)
            :checker   (:checker workload)
```

多考虑一下generator...我们知道它将处理两个阶段：添加和最终读取。我们也知道我们想要读取成功，这意味着我们想让集群正常并且恢复那一点，因此我们将在`add`阶段执行普通的分区操作，然后停止分区，等待一会让集群恢复，最终执行我们的读操作。`gen/phases`帮助我们编写这些类型的多阶段generators。

```clj
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis
                                (cycle [(gen/sleep 5)
                                        {:type :info, :f :start}
                                        (gen/sleep 5)
                                        {:type :info, :f :stop}]))
                              (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (gen/once {:type :info, :f :stop}))
                         (gen/log "Waiting for recovery")
                         (gen/sleep 10)
                         (gen/clients (:final-generator workload)))})))
```
让我们试一下，看看会发生什么？


```
$ lein run test --time-limit 10 --concurrency 10 -r 1/2
...
NFO [2018-02-04 22:13:53,085] jepsen worker 2 - jepsen.util 2	:invoke	:add	0
INFO [2018-02-04 22:13:53,116] jepsen worker 2 - jepsen.util 2	:ok	:add	0
INFO [2018-02-04 22:13:53,361] jepsen worker 2 - jepsen.util 2	:invoke	:add	1
INFO [2018-02-04 22:13:53,374] jepsen worker 2 - jepsen.util 2	:ok	:add	1
INFO [2018-02-04 22:13:53,377] jepsen worker 4 - jepsen.util 4	:invoke	:add	2
INFO [2018-02-04 22:13:53,396] jepsen worker 3 - jepsen.util 3	:invoke	:add	3
INFO [2018-02-04 22:13:53,396] jepsen worker 4 - jepsen.util 4	:ok	:add	2
INFO [2018-02-04 22:13:53,410] jepsen worker 3 - jepsen.util 3	:ok	:add	3
...
INFO [2018-02-04 22:14:06,934] jepsen nemesis - jepsen.generator Healing cluster
INFO [2018-02-04 22:14:06,936] jepsen nemesis - jepsen.util :nemesis	:info	:stop	nil
INFO [2018-02-04 22:14:07,142] jepsen nemesis - jepsen.util :nemesis	:info	:stop	:network-healed
INFO [2018-02-04 22:14:07,143] jepsen nemesis - jepsen.generator Waiting for recovery
...
INFO [2018-02-04 22:14:17,146] jepsen worker 4 - jepsen.util 4	:invoke	:read	nil
INFO [2018-02-04 22:14:17,153] jepsen worker 4 - jepsen.util 4	:ok	:read	#{0 7 20 27 1 24 55 39 46 4 54 15 48 50 21 31 32 40 33 13 22 36 41 43 29 44 6 28 51 25 34 17 3 12 2 23 47 35 19 11 9 5 14 45 53 26 16 38 30 10 18 52 42 37 8 49}
...
INFO [2018-02-04 22:14:29,553] main - jepsen.core {:valid? true,
 :lost "#{}",
 :recovered "#{}",
 :ok "#{0..55}",
 :recovered-frac 0,
 :unexpected-frac 0,
 :unexpected "#{}",
 :lost-frac 0,
 :ok-frac 1}


Everything looks good! ヽ(‘ー`)ノ
```

看上面的 55个添加操作，所有的添加都在最终读取中保存完整，如果有任何数据丢了，他们将会显示在`:lost`集合中

让我们将线性的寄存器重写为workload，因此它将与设置测试相同。

```clj
(defn register-workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:linear   (checker/linearizable {:model     (model/cas-register)
                                                    :algorithm :linear})
                   :timeline (timeline/html)}))
```


我们忘记性能展示图了。这些图对于每次测试似乎是有用的，因此我们将其排除在workload外，对于这个特殊的workload，我们需要线性一致性和HTML 时序图的独立checker。下一节，我们需要并发的generator

```clj
   :generator (independent/concurrent-generator
                10
                (range)
                (fn [k]
                  (->> (gen/mix [r w cas])
                       (gen/limit (:ops-per-key opts)))))})
```

这个generator比之前的更简单！nemesis、rate limiting和time limits 通过`etcd-test`来应用，因此我们可以将它们排除在workload之外。我们这儿也不需要 :final-generator,因此我们保留一个空白--"nil"，这个generator意味着啥也不做。

在workload之间切换，让我们起一个简短的名字

```clj
(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"set"      set/workload
   "register" register-workload})
```


现在，让我们避免在`etcd-test`中指定register，纯粹的让workdload来处理。我们将采用字符串workload选型，让它去查看适当的workload函数，然后使用`opts`调用来简历适当的workload。我们也更新我们的测试名称，以包含workload名称。

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key
      :workload     Type of workload."
  [opts]
  (let [quorum    (boolean (:quorum opts))
        workload  ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name       (str "etcd q=" quorum " "
                             (name (:workload opts)))
            :quorum     quorum
            :os         debian/os
            :db         (db "v3.1.5")
            :nemesis    (nemesis/partition-random-halves)
            :client     (:client workload)
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :workload (:checker workload)})
   ...
```

现在，让我们给CLI传递workload选项

```clj
(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ...
```


我们用`:missing`使tools.cli持续提供一些value，`cli/one-of`是一个缩写，它用来确保在map中该值是一个有效的key；它给我们一些有用的错误信息。现在如果我们不带workload来运行测试，它将告诉我们需要选择一个有效的workload。

```bash
$ lein run test --time-limit 10 --concurrency 10 -r 1/2
--workload Must be one of register, set
```

并且我们只需要按一下开关，就可以运行任一workload

```bash
$ lein run test --time-limit 10 --concurrency 10 -r 1/2 -w set
...
$ lein run test --time-limit 10 --concurrency 10 -r 1/2 -w register
...
```

就我们这堂课而言，你可以试想下，将register 测试移动到它自己的命名空间中，并将set 测试拆分使用独立键，谢谢阅读！

