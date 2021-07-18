# 数据库自动化

在单个Jepsen测试中，`DB`封装了用于设置和拆除我们所测试的数据库、队列或者其他分布式系统的代码。我们可以手动执行设置和拆除，但是让Jepsen处理它可以让我们在持续集成（CI）系统中运行测试、参数化数据库配置和连续地从头开始运行多个测试，等等。

在`src/jepsen/etcdemo.clj`中，我们需要使用`jepsen.db`、`jepsen.control`、`jepsen.control.util`和`jepsen.os.debian`命名空间，每个名称有别名作为简称。`clojure.string`将帮助我们为etcd建立配置字符串。我们还将从`clojure.tools.logging`中引入所有功能，为我们提供log功能，例如`info`，`warn`等。

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

然后，在给定特定版本字符串的情况下，我们将编写一个构造Jepsen `DB`的函数。

```clj
(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version))

    (teardown! [_ test node]
      (info node "tearing down etcd"))))
```

如上代码块所示，`（defn db ...`之后的字符串是*文档字符串* ，记录了函数的行为。 当获得`version`时，`db`函数使用`reify`构造一个满足Jepsen的`DB`协议的新对象（来自`db`命名空间）。该协议指定所有数据库必须支持的两个功能：`(setup！db test node)`和`(teardown! db test node)`，分别代表设置和拆除数据这两大功能。 我们提供存根（stub）实现在这里，它仅仅是输出一条参考消息日志。

现在，我们将通过添加`:os`来扩展默认的`noop-test`，以告诉Jepsen如何处理操作系统设置，以及一个`:db`，我们可以使用刚编写的`db`函数来构造。我们将测试etcd版本`v3.1.5`。

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os   debian/os
          :db   (db "v3.1.5")
          :pure-generators true}))
```

跟所有Jepsen测试一样，`noop-test`是一个有诸如`:os`, `:name`和`:db`等键的映射表。有关测试结构的概述详见[jepsen.core](../../jepsen/src/jepsen/core.clj)，有关测试的完整定义详见`jepsen.core/run`。

当前`noop-test`只有这些键的一些存根实现。但是我们可以用`merge`来构建一份赋予这些键*新*值的`noop-test`映射表的拷贝。

如果运行此测试，我们将看到Jepsen使用我们的代码来设置debian，假装拆除并安装etcd，然后启动其工作者。

```bash
$ lein run test
...
INFO [2017-03-30 10:08:30,852] jepsen node n2 - jepsen.os.debian :n2 setting up debian
INFO [2017-03-30 10:08:30,852] jepsen node n3 - jepsen.os.debian :n3 setting up debian
INFO [2017-03-30 10:08:30,852] jepsen node n4 - jepsen.os.debian :n4 setting up debian
INFO [2017-03-30 10:08:30,852] jepsen node n5 - jepsen.os.debian :n5 setting up debian
INFO [2017-03-30 10:08:30,852] jepsen node n1 - jepsen.os.debian :n1 setting up debian
INFO [2017-03-30 10:08:52,385] jepsen node n1 - jepsen.etcdemo :n1 tearing down etcd
INFO [2017-03-30 10:08:52,385] jepsen node n4 - jepsen.etcdemo :n4 tearing down etcd
INFO [2017-03-30 10:08:52,385] jepsen node n2 - jepsen.etcdemo :n2 tearing down etcd
INFO [2017-03-30 10:08:52,385] jepsen node n3 - jepsen.etcdemo :n3 tearing down etcd
INFO [2017-03-30 10:08:52,385] jepsen node n5 - jepsen.etcdemo :n5 tearing down etcd
INFO [2017-03-30 10:08:52,386] jepsen node n1 - jepsen.etcdemo :n1 installing etcd v3.1.5
INFO [2017-03-30 10:08:52,386] jepsen node n4 - jepsen.etcdemo :n4 installing etcd v3.1.5
INFO [2017-03-30 10:08:52,386] jepsen node n2 - jepsen.etcdemo :n2 installing etcd v3.1.5
INFO [2017-03-30 10:08:52,386] jepsen node n3 - jepsen.etcdemo :n3 installing etcd v3.1.5
INFO [2017-03-30 10:08:52,386] jepsen node n5 - jepsen.etcdemo :n5 installing etcd v3.1.5
...
```

看到了版本字符串`"v3.1.5"`是怎么从`etcd-test`传递到`db`，最终被reify表达式获取使用的吗？这就是我们*参数化*Jepsen测试的方式，因此相同的代码可以测试多个版本或选项。另请注意对象`reify`返回的结果在其词法范围内关闭，*记住*`version`的值。

## 安装数据库

有了已经准备好的DB函数框架，就该实际安装一些东西了。让我们快速看一下[etcd的安装说明](https://github.com/coreos/etcd/releases/tag/v3.1.5)。 看来我们需要下载一个tarball，将其解压缩到目录中，为API版本设置一个环境变量，然后使用它运行`etcd`二进制文件。

想要安装这些包，必须先获取root权限。因此我们将使用`jepsen.control/su`来获取root特权。请注意，`su`（及其伴随的`sudo`、`cd`等等）确立的是*动态*而非*词法*范围，他们的范围不仅作用于包起来部分的代码，还包括所有函数的调用栈。然后，我们将使用`jepsen.control.util/install-archive!`来下载etcd安装文件，并将其安装到`/opt/etcd`目录下。

```clj
(def dir "/opt/etcd")

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/" version
                       "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))))

    (teardown! [_ test node]
      (info node "tearing down etcd"))))
```
在我们移除etcd目录的时候，我们正在使用`jepsen.control/su`变成root用户（通过sudo）。`jepsen.control`提供了全面的领域特定语言（DSL）在远程节点上执行任意的shell命令。

现在，`lein run test`将自动安装etcd。请注意，Jepsen在所有节点上同时进行“设置”和“拆卸”。这可能需要一些时间，因为每个节点都必须下载tarball，但是在以后的运行中，Jepsen将重新使用磁盘上缓存的tarball。

## 启动数据库


根据etcd[集群化命令](https://coreos.com/etcd/docs/latest/v2/clustering.html)，我们需要生成一串形如`"ETCD_INITIAL_CLUSTER="infra0=http://10.0.1.10:2380,infra1=http://10.0.1.11:2380,infra2=http://10.0.1.12:2380"`的字符串。这样我们的节点才知道哪些节点是集群的一部分。让我们写几个短函数来构造这些字符串：

```clj
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

`->>`是Clojure的一个宏，将一个形式插入到下一个形式作为最后一个参数（因为作用类似于缝衣服时候的穿线，因此在英文中命名这个宏为“threading”）。因此，`(->> test :nodes)`就变成了`(:nodes test)`，而`(->> test
:nodes (map-indexed (fn ...)))`就变成了`(map-indexed (fn ...) (:nodes test))`，以此类推。普通的函数调用看起来就是“由内而外”，但是`->>`这个宏让我们“按顺序”编写一系列操作，类似于一个面向对象语言的`foo.bar().baz()`表示形式。

在函数`initial-cluster`中，我们从test映射表中获取到了数个节点，并将每个节点通过Clojure内置的map映射为相应的字符串：节点名称、“=”和节点的peer的url。然后我们将这些字符串用（英文）逗号合并起来。

准备好之后，我们会告诉数据库怎么以守护进程的方式启动。我们可以使用初始化脚本或者服务来启动和关闭程序，不过既然我们正在使用的是一个单纯的二进制文件，我们将使用Debian的`start-stop-daemon`命令在后台运行`etcd`。

我们还需要一些常量：etcd二进制文件名、日志输出的地方和存储pidfile文件的地方。

```clj
(def dir     "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))
```

现在我们将使用`jepsen.control.util`内用于启动和关闭守护进程的函数来启动etcd。根据[documentation](https://coreos.com/etcd/docs/latest/v2/clustering.html)，我们将需要提供一个节点的名称、用于监听客户端和peer节点的数个URLs和集群初始状态。

```clj
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
          :--name                         (name node)
          :--listen-peer-urls             (peer-url   node)
          :--listen-client-urls           (client-url node)
          :--advertise-client-urls        (client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (peer-url node)
          :--initial-cluster              (initial-cluster test))

        (Thread/sleep 10000)))
```

我们将在启动集群之后调用sleep函数让程序暂停一会儿，这样集群才能有机会完全启动并执行初始的网络握手。

## 拆除

为了确保每次运行都是从零开始，即使先前的运行崩溃了，Jepsen也会在测试开始之前进行DB拆除，然后再进行设置。然后在测试结束时将数据库再次撕毁。要拆除，我们将使用`stop-daemon!`，然后删除etcd目录，以便将来的运行不会意外地从当前运行中读取数据。

```clj
    (teardown! [_ test node]
      (info node "tearing down etcd")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))))
```

我们使用`jepsen.control/exec`运行shell命令：`rm -rf`。Jepsen会自动指定使用`exec`，以便在`db/setup!`期间设置的`node`上运行一些操作，但是我们可以根据需要连接到任意节点。请注意，`exec`可以混合使用字符串、数字和关键字的任意组合，它将它们转换为字符串并执行适当的shell转义。如果需要，可以将`jepsen.control/lit`用于未转义的文本字符串。`:>`和`:>>`是Clojure的关键字，被`exec`接收后可以执行shell的重定向。对于需要配置的数据库，这是将配置文件写到磁盘的一个简单方法。

现在让我们试试看！

```bash
$ lein run test
NFO [2017-03-30 12:08:19,755] jepsen node n5 - jepsen.etcdemo :n5 installing etcd v3.1.5
INFO [2017-03-30 12:08:19,755] jepsen node n1 - jepsen.etcdemo :n1 installing etcd v3.1.5
INFO [2017-03-30 12:08:19,755] jepsen node n2 - jepsen.etcdemo :n2 installing etcd v3.1.5
INFO [2017-03-30 12:08:19,755] jepsen node n4 - jepsen.etcdemo :n4 installing etcd v3.1.5
INFO [2017-03-30 12:08:19,855] jepsen node n3 - jepsen.etcdemo :n3 installing etcd v3.1.5
INFO [2017-03-30 12:08:20,866] jepsen node n4 - jepsen.control.util starting etcd
INFO [2017-03-30 12:08:20,866] jepsen node n1 - jepsen.control.util starting etcd
INFO [2017-03-30 12:08:20,866] jepsen node n5 - jepsen.control.util starting etcd
INFO [2017-03-30 12:08:20,866] jepsen node n2 - jepsen.control.util starting etcd
INFO [2017-03-30 12:08:20,963] jepsen node n3 - jepsen.control.util starting etcd
...
```

上面的运行结果看起来很棒。我们可以通过在测试后检查etcd目录是否为空来确认`teardown`是否已完成工作。

```bash
$ ssh n1 ls /opt/etcd
ls: cannot access /opt/etcd: No such file or directory
```

## 日志文件

等等——如果我们在每次运行后删除etcd的文件，我们如何确定数据库做了什么？如果我们可以在清理之前下载数据库日志的副本，那就太好了。为此，我们将使用`db/LogFiles`协议，并返回要下载的日志文件路径的列表。

```clj
(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      ...)

    (teardown! [_ test node]
      ...)

    db/LogFiles
    (log-files [_ test node]
      [logfile])))
```

现在，当我们运行测试时，我们将为每个节点找到一个日志副本，存储在本地目录`store/latest/<node-name>/`中。 如果我们在设置数据库时遇到问题，我们可以检查那些日志以查看出了什么问题。

```bash
$ less store/latest/n1/etcd.log
...
2018-02-02 11:36:51.848330 I | raft: 5440ff22fe632778 became leader at term 2
2018-02-02 11:36:51.848360 I | raft: raft.node: 5440ff22fe632778 elected leader 5440ff22fe632778 at term 2
2018-02-02 11:36:51.860295 I | etcdserver: setting up the initial cluster version to 3.1
2018-02-02 11:36:51.864532 I | embed: ready to serve client requests
...
```

寻找“选举产生的领导者”这一行，这表明我们的节点成功地形成了集群。如果您的etcd节点彼此看不到，请确保使用正确的端口名，并在`node-url`中使用`http://`而不是`https://`，并且该节点可以互相ping通。

准备好了数据库之后，接下来该[编写客户端](03-cn-client.md)。
