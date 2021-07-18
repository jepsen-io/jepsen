# 编写一个客户端

一个Jepsen *client*接收*调用操作*（英文术语invocation operations），然后将其应用于要测试的系统，并返回相应的执行完成结果（这一阶段称为completion operation）。 对于我们的etcd测试，我们可以将系统建模为单个寄存器：一个持有整数的特定键。针对该寄存器的操作可能是`read`、`write`和`compare-and-set`，我们可以像这样建模：

```clj
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
```

在上面这个代码块中，是几个构建Jepsen*操作*的函数。这是对数据库可能进行的操作的一种抽象表示形式。`:invoke`表示我们将进行*尝试操作*-完成后，我们将使用一种类型，比如`:ok`或`:fail`来告诉我们发生了什么。`:f`告诉我们正在应用什么函数到数据库-例如，我们要对数据执行读取或写入操作。`:f`可以是*任何*值-Jepsen并不知道它们的含义。

函数调用通常都是通过入口参数和返回值来进行参数化。而Jepsen的操作是通过`:value`来进行参数化。Jepsen不会去检查`:value`，因此`:value`后可以跟任意指定的参数。我们使用函数write的`:value`来指定写入的值，用函数read的`:value`来指定我们（最终）读取的值。 当read被调用的时候，我们还不知道将读到什么*值*，因此我们将保持函数read的`:value`为空。

这些函数能被`jepsen.generator`用于构建各种各样的调用，分别用于读取、写入和CAS。注意函数read的`:value`是空的-由于无法预知到能读取到什么值，所以将其保留为空。直到客户端读到了一个特定的数值后，在completion operation阶段，函数read的参数才会被填充。

## 连接到数据库

现在我们需要拿到这些操作然后将其*应用*到etcd。我们将会使用[Verschlimmbessergung](https://github.com/aphyr/verschlimmbesserung)这个库来与etcd进行通信。我们将从引入Verschlimmbesserung开始，然后编写一个Jepsen Client协议的空实现：

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
...
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    this)

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))
```

如上面代码块所示，`defrecord`定义了一种新的数据结构类型，称之为`Client`。每个`Client`都有一个叫做`conn`的字段，用于保持到特定网络服务器的连接。这些客户端函数支持Jepsen的客户端协议，就像对这个协议“具象化”（英文术语reify）了一样，还提供客户端功能的具体实现。

Jepsen的客户端有五部分的生命周期。我们先从*种子*客户端`(client)`开始。当我们调用客户端的`open!`的时候，我们得到跟一个特定节点绑定的客户端的*副本*。`setup!`函数测试所需要的所有数据结构-例如创建表格或者设置固件。`invoke!`将操作应用到系统然后返回相应的完成操作。`teardown!`会清理`setup!`可能创建的任何表格。`close!`会断开所有网络连接并完成客户端的生命周期。

当需要将客户端添加到测试中时,我们使用`(Client.)`来构建一个新的客户端，并传入`nil`作为`conn`的值。请记住，我们最初的种子客户端没有连接。Jepsen后续会调用`open!`来获取已连接的客户端。

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name   "etcd"
          :os     debian/os
          :db     (db "v3.1.5")
          :client (Client. nil)}))
```

现在，让我们来完成`open!`函数连接到etcd的功能。[Verschlimmbesserung docs](https://github.com/aphyr/verschlimmbesserung#usage)的这个教程告诉了我们创建客户端所需要函数。这个函数使用`(connect url)`来创建一个etcd客户端。其中`conn`里面存储的正是我们需要的客户端。此处，我们设置Verschlimmbesserung调用的超时时间为5秒。

```clj
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (client-url node)
                                 {:timeout 5000})))

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]
    ; If our connection were stateful, we'd close it here. Verschlimmmbesserung
    ; doesn't actually hold connections, so there's nothing to close.
    ))

(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (Client. nil)}))
```

请记住，最初的客户端并*没有任何连接*。就像一个干细胞一样，它具备称为活跃的客户端的*潜力*，但是不会直接承担任何工作。我们调用`(Client. nil)`来构建初始客户端，其连接只有当Jepsen调用`open!`的时候才会被赋值。

## 客户端读操作

现在我们需要真正地开始用客户端*做*点事情了。首先从15秒的读操作开始，随机地错开大约一秒钟。 我们将引入`jepsen.generator`来调度操作。

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

并编写一个简单的生成器：执行一系列的读取操作，并将它们错开一秒钟左右，仅将这些操作提供给客户端（而不是给nemesis，它还有其他职责），然后在15秒后停止。

```clj
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
          :generator       (->> r
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

上面这段代码执行后将抛出一堆错误，因为我们尚未告诉客户的*如何*去解读这些到来的读操作。

```bash
$ lein run test
...
WARN [2020-09-21 20:16:33,150] jepsen worker 0 - jepsen.generator.interpreter Process 0 crashed
clojure.lang.ExceptionInfo: throw+: {:type :jepsen.client/invalid-completion, :op {:type :invoke, :f :read, :value nil, :time 26387538, :process 0}, :op' nil, :problems ["should be a map" ":type should be :ok, :info, or :fail" ":process should be the same" ":f should be the same"]}
```

现在这个版本客户端的`invoke!`函数，接收到调用操作，但是没有进行任何相关处理，返回的是一个`nil`结果。Jepsen通过这段日志告诉我们，op应该是一个映射表，尤指带有相应的`:type`字段、`:process`字段和`:f`字段的映射表。简而言之，我们必须构建一个完成操作来结束本次调用操作。如果操作成功，我们将使用类型`:ok`来构建此完成操作；如果操作失败，我们将使用类型`:fail`来构建；或者如果不确定则使用`:info`来构建。`invoke`可以抛出一个异常，会自动被转为一个`:info`完成操作。

现在我们从处理读操作开始。我们将使用`v/get`来读取一个键的值。我们可以挑选任意一个名字作为这个键的名称，比如“foo”。

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (v/get conn "foo"))))
```

我们根据操作的`:f`字段来给Jepsen分派任务。当`:f`是`:read`的时候，我们调用invoke操作并返回其副本，带有`:type`、`:ok`和通过读取寄存器“foo”得到的值。

```bash
$ lein run test
...
INFO [2017-03-30 15:28:17,423] jepsen worker 2 - jepsen.util 2  :invoke :read nil
INFO [2017-03-30 15:28:17,427] jepsen worker 2 - jepsen.util 2  :ok :read nil
INFO [2017-03-30 15:28:18,315] jepsen worker 0 - jepsen.util 0  :invoke :read nil
INFO [2017-03-30 15:28:18,320] jepsen worker 0 - jepsen.util 0  :ok :read nil
INFO [2017-03-30 15:28:18,437] jepsen worker 4 - jepsen.util 4  :invoke :read nil
INFO [2017-03-30 15:28:18,441] jepsen worker 4 - jepsen.util 4  :ok :read nil
```

这下好多了！由于“foo”这个键尚未被创建，因此读到的值都是`nil`。为了更改这个值，我们将会添加一些写操作到生成器上。

## 写操作

我们将使用`(gen/mix [r w])`，来更改我们的生成器以将读写随机组合。

```clj
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
          :generator       (->> (gen/mix [r w])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

为了处理这些写操作，我们将使用`v/reset!`并返回带有`:type`和`:ok`的操作。如果`reset!`失败，那么就会抛出错误，而Jepsen的机制就是自动将错误转为`:info`标注的崩溃。

```clj
    (invoke! [this test op]
               (case (:f op)
                 :read (assoc op :type :ok, :value (v/get conn "foo"))
                 :write (do (v/reset! conn "foo" (:value op))
                            (assoc op :type :ok))))
```

我们会通过观察下面这个测试来确认写操作成功了。

```bash
$ lein run test
INFO [2017-03-30 22:14:25,428] jepsen worker 4 - jepsen.util 4  :invoke :write  0
INFO [2017-03-30 22:14:25,439] jepsen worker 4 - jepsen.util 4  :ok :write  0
INFO [2017-03-30 22:14:25,628] jepsen worker 0 - jepsen.util 0  :invoke :read nil
INFO [2017-03-30 22:14:25,633] jepsen worker 0 - jepsen.util 0  :ok :read "0"
```

啊，看来我们这边遇到了点小困难。etcd处理的是字符串，不过我们喜欢与数字打交道。我们可以引入一个序列化库（Jepsen就包含了一个简单的序列化库`jepsen.codec`），不过既然我们现在处理的只是整数和nil，我们可以摆脱序列化库而直接使用Java的内置`Long.parseLong(String str)`方法。

```clj
(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

...

  (invoke! [_ test op]
    (case (:f op)
      :read  (assoc op :type :ok, :value (parse-long (v/get conn "foo")))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type :ok))))
```

注意只有当调用`(when s ...)`字符串是逻辑true的时候（即字符串非空），才会调用parseLong函数。如果`when`匹配不上，则返回`nil`，这样我们就可以在无形之中忽略`nil`值。

```bash
$ lein run test
...
INFO [2017-03-30 22:26:45,322] jepsen worker 4 - jepsen.util 4  :invoke :write  1
INFO [2017-03-30 22:26:45,341] jepsen worker 4 - jepsen.util 4  :ok :write  1
INFO [2017-03-30 22:26:45,434] jepsen worker 2 - jepsen.util 2  :invoke :read nil
INFO [2017-03-30 22:26:45,439] jepsen worker 2 - jepsen.util 2  :ok :read 1
```

现在还剩一种操作没去实现：比较并设置。

## 比较替换（CaS）

添加完CaS操作后，我们就结束本节关于客户端内容的介绍。

```clj
      (gen/mix [r w cas])
```

处理CaS会稍显困难。Verschlimmbesserung提供了`cas!`函数，入参包括连接、键、键映射的旧值和键映射的新值。`cas!`只有当入参的旧值匹配该键对应的当前值的时候，才会将入参的键设置为入参的新值，然后返回一个详细的映射表作为响应。如果CaS操作失败，将返回false。这样我们就可以将其用于决定CaS操作的`:type`字段。

```clj
  (invoke! [_ test op]
    (case (:f op)
      :read  (assoc op :type :ok, :value (parse-long (v/get conn "foo")))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type :ok))
      :cas (let [[old new] (:value op)]
             (assoc op :type (if (v/cas! conn "foo" old new)
                               :ok
                               :fail)))))
```

这边的`let`绑定用于*解构*。它将操作的`:value`字段的一对值`[旧值 新值]`分开到`old`和`new`上。由于除了`false`和`nil`之外所有值都是表示逻辑true，我们可以使用`cas!`调用的结果作为`if`语句中的条件断言。

## Handling exceptions

如果你已经运行过几次Jepsen了，你可能会看到以下内容：

```bash
$ lein run test
...
INFO [2017-03-30 22:38:51,892] jepsen worker 1 - jepsen.util 1  :invoke :cas  [3 1]
WARN [2017-03-30 22:38:51,936] jepsen worker 1 - jepsen.core Process 1 indeterminate
clojure.lang.ExceptionInfo: throw+: {:errorCode 100, :message "Key not found", :cause "/foo", :index 11, :status 404}
  at slingshot.support$stack_trace.invoke(support.clj:201) ~[na:na]
  ...
```

如果我们试图对不存在的键进行CaS操作，Verschlimmbesserung会抛出异常来告诉我们不能修改不存在的东西。这不会造成我们的测试结果返回误报。Jepsen会将这种异常解读为不确定的`:info`结果，并对这种结果的置若罔闻。然而，当看到这个异常时候，我们*知道*CaS的数值修改失败了。所以我们可以将其转为已知的错误。我们将引入`slingshot`异常处理库来捕获这个特别的错误码。

```clj
(ns jepsen.etcdemo
  (:require ...
            [slingshot.slingshot :refer [try+]]))
```

引入之后，将我们的`:cas`放进一个try/catch代码块中。

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long (v/get conn "foo")))
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

[:errorCode 100]形式的代码告诉Slingshot去捕获有这个特定的错误码的异常，然后将其赋值给`ex`。我们已经添加了一个额外的`:error`字段到我们的操作中。只要还考虑正确性，这件事情做不做都无所谓。但是在我们查看日志的时候，这能帮助我们理解当时到底发生了什么。Jepsen将会把错误打印在日志的行末。

```bash
$ lein run test
...
INFO [2017-03-30 23:00:50,978] jepsen worker 0 - jepsen.util 0  :invoke :cas  [1 4]
INFO [2017-03-30 23:00:51,065] jepsen worker 0 - jepsen.util 0  :fail :cas  [1 4] :not-found
```

这下看上去更加清楚了。通常，我们将从编写最简单的代码开始，然后允许Jepsen为我们处理异常。 一旦我们对测试出错的可能的情况有大概了解，我们可以为那些错误处理程序和语义引入特殊的
失败案例。

```bash
...
INFO [2017-03-30 22:38:59,278] jepsen worker 1 - jepsen.util 11 :invoke :write  4
INFO [2017-03-30 22:38:59,286] jepsen worker 1 - jepsen.util 11 :ok :write  4
INFO [2017-03-30 22:38:59,289] jepsen worker 4 - jepsen.util 4  :invoke :cas  [2 2]
INFO [2017-03-30 22:38:59,294] jepsen worker 1 - jepsen.util 11 :invoke :read nil
INFO [2017-03-30 22:38:59,297] jepsen worker 1 - jepsen.util 11 :ok :read 4
INFO [2017-03-30 22:38:59,298] jepsen worker 4 - jepsen.util 4  :fail :cas  [2 2]
INFO [2017-03-30 22:38:59,818] jepsen worker 4 - jepsen.util 4  :invoke :write  1
INFO [2017-03-30 22:38:59,826] jepsen worker 4 - jepsen.util 4  :ok :write  1
INFO [2017-03-30 22:38:59,917] jepsen worker 1 - jepsen.util 11 :invoke :cas  [1 2]
INFO [2017-03-30 22:38:59,926] jepsen worker 1 - jepsen.util 11 :ok :cas  [1 2]
```

注意到某些CaS操作失败，而其他成功了吗？有些会失败很正常，事实上，这正是我们想看到的。我们预计某些CaS操作会失败，因为断定的旧值与当前值不匹配，但有几个（概率大概是1/5，因为在任何时候，寄存器的值都只可能5个可能性）应该成功。另外，尝试一些我们任务不可能成功的操作其实是值得的，因为如果它们*真的*成功，则表明存在一致性冲突。

有了可以执行操作的客户端后，现在可以着手使用[检查器](04-cn-checker.md)分析结果了。
