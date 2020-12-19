# 正确性校验

通过生成器和客户端执行一些操作，我们获取到了用于分析正确性的历史记录。Jepsen使用*model*代表系统的抽象行为，*checker*来验证历史记录是否符合该模型。我们需要`knossos.model`和`jepsen.checker`：

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
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```

还记得我们如何构建读、写和cas操作吗？

```clojure
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
```

Jepsen并不知道`:f :read`或`:f :cas`的含义，就其而言,他们可以是任意值。然而，当它基于`(case (:f op) :read ...)`进行控制流转时，我们的*client*知道如何解释这些操作。现在，我们需要一个能够理解这些相同操作的系统模型。Knossos已经为我们定义好了模型数据类型，它接受一个模型或者操作作为输入进行运算，并返回该操作产生的新模型。*knossos.model*内部代码如下：

```clojure
(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model.
        Models should be a pure, deterministic function of their state and an
        operation's :f and :value."))
```

结果发现Knossos检查器为锁和寄存器等东西定义了一些常见的模型。下面的内容是一个[cas寄存器](https://github.com/jepsen-io/knossos/blob/443a5a081c76be315eb01c7990cc7f1d9e41ed9b/src/knossos/model.clj#L66-L80)--正是我们需要建模的数据类型

```clojure
(defrecord CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str "can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str "can't read " (:value op)
                                  " from register " value))))))
```

只要*knossos*为我们正在检测的组件提供了模型，我们就不需要在测试中写cas寄存器。这只是为了你可以看到表面上一切顺利，其实是依靠底层怎么运行的。

此defrecord定义了一个名为*CASRegister*的新的数据类型，它拥有唯一不变的字段，名为*value*。它实现了我们之前讨论的`Model`接口，它的`step`函数接收当前寄存器`r`和操作`op`作为参数。当我们需要写入新值时，只需要简单返回一个已经赋值的`CASRegister`。为了对两个值进行cas，我们在操作中将当前值和新值分开，如果当前值和新值相匹配，则构建一个带有新值的寄存器。如果它们不匹配，则返回带有`inconsistent`的特定的模型类型，它表明上一操作不能应用于寄存器。读操作也是类似，除了我们始终允许读取到`nil`这一点。这允许我们有从未返回过的读操作历史。

为了分析历史操作，我们需要为测试定义一个`:checker`，同时需要提供一个`:model`来指明系统*应该*如何运行。  
`checker/linearizable`使用Knossos线性checker来验证每一个操作是否自动处于调用和返回之间的位。线性checker需要一个模型并指明一个特定的算法，然后在选项中将map传递给该算法。

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
          :checker         (checker/linearizable
                             {:model     (model/cas-register)
                              :algorithm :linear})
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

运行测试，我们可以验证checker的结果：

```bash
$ lein run test
...
INFO [2019-04-17 17:38:16,855] jepsen worker 0 - jepsen.util 0  :invoke :write  1
INFO [2019-04-17 17:38:16,861] jepsen worker 0 - jepsen.util 0  :ok :write  1
...
INFO [2019-04-18 03:53:32,714] jepsen test runner - jepsen.core {:valid? true,
 :configs
 ({:model #knossos.model.CASRegister{:value 3},
   :last-op
   {:process 1,
    :type :ok,
    :f :write,
    :value 3,
    :index 29,
    :time 14105346871},
   :pending []}),
 :analyzer :linear,
 :final-paths ()}


Everything looks good! ヽ(‘ー`)ノ
```

历史记录中最后的操作是write `1`，可以确信，checker中的最终值也是`1`，该历史记录是线性一致的。

# 多checkers

checkers能够渲染多种类型的输出--包括数据结构、图像、或者可视化交互动画。例如：如果我们安装了`gnuplot`，Jepsen可以帮我们生成吞吐量和延迟图。让我们使用`checker/compose`来进行线性分析并生成性能图吧！

```clojure
         :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear (checker/linearizable {:model     (model/cas-register)
                                                     :algorithm :linear})})
```
```bash
$ lein run test
...
$ open store/latest/latency-raw.png
```

我们也可以生成历史操作HTML可视化界面。我们来添加`jepsen.checker.timeline`命名空间吧！

```clojure
(ns jepsen.etcdemo
  (:require ...
            [jepsen.checker.timeline :as timeline]
            ...))
```

给checker添加测试：

```clojure
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear (checker/linearizable
                                {:model     (model/cas-register)
                                 :algorithm :linear})
                      :timeline (timeline/html)})
```

现在我们可以绘制不同流程随时间变化执行的操作图，其中包括成功的、失败的以及崩溃的操作等等。

```bash
$ lein run test
...
$ open store/latest/timeline.html
```

现在我们已经通过测试，接下来详述系统中的[故障引入](https://github.com/jaydenwen123/jepsen/blob/main/doc/cn_tutorial/05-cn-nemesis.md)