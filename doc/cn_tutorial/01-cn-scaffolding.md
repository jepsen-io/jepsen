# 测试脚手架

在本新手教程中，我们打算为etcd编写一个测试。etcd是一个分布式共识系统。在此，我想建议各位在学习过程中能自己*亲手敲*一下代码，即便一开始还不是特别理解所有内容。如此一来既能帮你学得更快，也不会在我们开始修改更复杂函数代码时而感到茫然。

我们首先在任意目录下创建一个新的[Leiningen](https://github.com/technomancy/leiningen)（音读['laɪnɪŋən]）项目。

```bash
$ lein new jepsen.etcdemo
Generating a project called jepsen.etcdemo based on the 'default' template.
The default template is intended for library projects, not applications.
To see other templates (app, plugin, etc), try `lein help new`.
$ cd jepsen.etcdemo
$ ls
CHANGELOG.md  doc/  LICENSE  project.clj  README.md  resources/  src/  test/
```

正如任何一个新创建的[Clojure](https://zh.wikipedia.org/wiki/Clojure)（音读/ˈkloʊʒər/）项目那样，我们会得到一个空白的变更日志、一个用于建立文档的目录、一个Eclipse公共许可证副本、一个`project.clj`文件（该文件告诉`leiningen`如何构建和运行我们的代码）以及一个名为README的自述文件。`resources`目录是用于存放数据文件的地方，比如我们想进行测试的数据库的配置文件。`src`目录存放着源代码，并按照代码中命名空间的结构被组织成一系列目录和文件。`test`目录是用于存放测试代码的目录。值得一提的是，这*整个目录*就是一个“Jepsen测试”；`test`目录是沿袭大多数Clojure库的习惯生成，而在本文中，我们不会用到它。


我们将从编辑一个指定项目的依赖项和其他元数据的`project.clj`文件来开始。我们将增加一个`:main`命名空间，正如下面一段命令行所示。除了依赖于Clojure自身的语言库，我们还添加了Jepsen库和一个用于与etcd进行通信的Verschlimmbesserung库。

```clj
(defproject jepsen.etcdemo "0.1.0-SNAPSHOT"
  :description "A Jepsen test for etcd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.etcdemo
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1-SNAPSHOT"]
                 [verschlimmbesserung "0.1.3"]])
```

让我们先尝试用`lein run`来运行这个程序。

```bash
$ lein run
Exception in thread "main" java.lang.Exception: Cannot find anything to run for: jepsen.etcdemo, compiling:(/tmp/form-init6673004597601163646.clj:1:73)
...
```

运行完后看到这样的数据结果并不意外，因为我们尚未写任何实质性的代码让程序去运行。在`jepsen.etcdemo`命名空间下，我们需要一个main函数来接收命令行参数并运行测试。在`src/jepsen/etcdemo.clj`文件中我们定义如下main函数：

```clj
(ns jepsen.etcdemo)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (prn "Hello, world!" args))
```

Clojure默认接收跟在`lein run`指令后的所有参数作为`-main`函数的调用参数。main函数接收长度可变的参数（即“&”符号），参数列表叫做`args`。在上述这段代码中，我们在“Hello World”之后打印参数列表。

```bash
$ lein run hi there
"Hello, world!" ("hi" "there")
```

Jepsen囊括了一些用于处理参数、运行测试、错误处理和日志记录等功能的脚手架。现在不妨引入`jepsen.cli`命名空间，简称为`cli`，然后将我们的main函数转为一个Jepsen测试运行器。

```clj
(ns jepsen.etcdemo
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))


(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))
```

`cli/single-test-cmd`由`jepsen.cli`提供。它为测试解析命令行的参数并调用提供的`:test-fn`，然后应该返回一个包含Jepsen运行测试所需的所有信息的键值映射表（map）。在上面这个样例中，测试函数`etcd-test`从命令行中接受一些选项，然后用它们来填充一个什么都不处理的空测试（即`noop-test`）。

```bash
$ lein run
Usage: lein run -- COMMAND [OPTIONS ...]
Commands: test
```

如上述代码块所示，没有参数的话，`cli/run!`会输出一个基本的帮助信息，提醒我们它接收一个命令作为其第一个参数。现在让我们尝试添加一下test命令吧！

```bash
$ lein run test
13:04:30.927 [main] INFO  jepsen.cli - Test options:
 {:concurrency 5,
 :test-count 1,
 :time-limit 60,
 :nodes ["n1" "n2" "n3" "n4" "n5"],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil}}

INFO [2018-02-02 13:04:30,994] jepsen test runner - jepsen.core Running test:
 {:concurrency 5,
 :db
 #object[jepsen.db$reify__1259 0x6dcf7b6a "jepsen.db$reify__1259@6dcf7b6a"],
 :name "noop",
 :start-time
 #object[org.joda.time.DateTime 0x79d4ff58 "2018-02-02T13:04:30.000-06:00"],
 :net
 #object[jepsen.net$reify__3493 0xae3c140 "jepsen.net$reify__3493@ae3c140"],
 :client
 #object[jepsen.client$reify__3380 0x20027c44 "jepsen.client$reify__3380@20027c44"],
 :barrier
 #object[java.util.concurrent.CyclicBarrier 0x2bf3ec4 "java.util.concurrent.CyclicBarrier@2bf3ec4"],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil},
 :checker
 #object[jepsen.checker$unbridled_optimism$reify__3146 0x1410d645 "jepsen.checker$unbridled_optimism$reify__3146@1410d645"],
 :nemesis
 #object[jepsen.nemesis$reify__3574 0x4e6cbdf1 "jepsen.nemesis$reify__3574@4e6cbdf1"],
 :active-histories #<Atom@210a26b: #{}>,
 :nodes ["n1" "n2" "n3" "n4" "n5"],
 :test-count 1,
 :generator
 #object[jepsen.generator$reify__1936 0x1aac0a47 "jepsen.generator$reify__1936@1aac0a47"],
 :os
 #object[jepsen.os$reify__1176 0x438aaa9f "jepsen.os$reify__1176@438aaa9f"],
 :time-limit 60,
 :model {}}

INFO [2018-02-02 13:04:35,389] jepsen nemesis - jepsen.core Starting nemesis
INFO [2018-02-02 13:04:35,389] jepsen worker 1 - jepsen.core Starting worker 1
INFO [2018-02-02 13:04:35,389] jepsen worker 2 - jepsen.core Starting worker 2
INFO [2018-02-02 13:04:35,389] jepsen worker 0 - jepsen.core Starting worker 0
INFO [2018-02-02 13:04:35,390] jepsen worker 3 - jepsen.core Starting worker 3
INFO [2018-02-02 13:04:35,390] jepsen worker 4 - jepsen.core Starting worker 4
INFO [2018-02-02 13:04:35,391] jepsen nemesis - jepsen.core Running nemesis
INFO [2018-02-02 13:04:35,391] jepsen worker 1 - jepsen.core Running worker 1
INFO [2018-02-02 13:04:35,391] jepsen worker 2 - jepsen.core Running worker 2
INFO [2018-02-02 13:04:35,391] jepsen worker 0 - jepsen.core Running worker 0
INFO [2018-02-02 13:04:35,391] jepsen worker 3 - jepsen.core Running worker 3
INFO [2018-02-02 13:04:35,391] jepsen worker 4 - jepsen.core Running worker 4
INFO [2018-02-02 13:04:35,391] jepsen nemesis - jepsen.core Stopping nemesis
INFO [2018-02-02 13:04:35,391] jepsen worker 1 - jepsen.core Stopping worker 1
INFO [2018-02-02 13:04:35,391] jepsen worker 2 - jepsen.core Stopping worker 2
INFO [2018-02-02 13:04:35,391] jepsen worker 0 - jepsen.core Stopping worker 0
INFO [2018-02-02 13:04:35,391] jepsen worker 3 - jepsen.core Stopping worker 3
INFO [2018-02-02 13:04:35,391] jepsen worker 4 - jepsen.core Stopping worker 4
INFO [2018-02-02 13:04:35,397] jepsen test runner - jepsen.core Run complete, writing
INFO [2018-02-02 13:04:35,434] jepsen test runner - jepsen.core Analyzing
INFO [2018-02-02 13:04:35,435] jepsen test runner - jepsen.core Analysis complete
INFO [2018-02-02 13:04:35,438] jepsen results - jepsen.store Wrote /home/aphyr/jepsen/jepsen.etcdemo/store/noop/20180202T130430.000-0600/results.edn
INFO [2018-02-02 13:04:35,440] main - jepsen.core {:valid? true}

Everything looks good! ヽ(‘ー`)ノ
```

如上面展示的代码块所示，我们发现Jepsen启动了一系列的workers（类似于进程）。每一个worker负责执行针对数据库的操作。此外，Jepsen还启动了一个nemesis，用于制造故障。由于它们尚未被分配任何任务，所以它们马上便关闭了。Jepsen将这个简易测试的结果输出写到了`store`目录下，并打印出一个简要分析。

`noop-test`默认使用名为`n1`、`n2` ... `n5`的节点。如果你的节点有不一样的名称，该测试会因无法连接这些节点而失败。但是这并没关系。你可以在命令行中来指定这些节点名称：

```bash
$ lein run test --node foo.mycluster --node 1.2.3.4
```

亦或者通过传入一个文件名来达到相同目的。文件中要包含节点列表，且每行一个。如果你正在使用AWS Marketplace集群，一个名为`nodes`的文件已经生成于机器的home目录下，随时可用。

```bash
$ lein run test --nodes-file ~/nodes
```

如果你当前依然在不断地遇到SSH错误，你应该检查下你的SSH是否代理正在运行并且已经加载了所有节点的密钥。`ssh some-db-node`应该可以不用密码就连接上数据库。你也可以在命令行上重写对应的用户名、密码和身份文件。详见`lein run test --help`。

```bash
$ lein run test --help
#object[jepsen.cli$test_usage 0x7ddd84b5 jepsen.cli$test_usage@7ddd84b5]

  -h, --help                                                  Print out this message and exit
  -n, --node HOSTNAME             ["n1" "n2" "n3" "n4" "n5"]  Node(s) to run test on
      --nodes-file FILENAME                                   File containing node hostnames, one per line.
      --username USER             root                        Username for logins
      --password PASS             root                        Password for sudo access
      --strict-host-key-checking                              Whether to check host keys
      --ssh-private-key FILE                                  Path to an SSH identity file
      --concurrency NUMBER        1n                          How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes.
      --test-count NUMBER         1                           How many times should we repeat a test?
      --time-limit SECONDS        60                          Excluding setup and teardown, how long should a test run for, in seconds?
```

在本指导教程中，我们将全程使用`lein run test ...`来重新运行我们的Jepsen测试。每当我们运行一次测试，Jepsen将在`store/`下创建一个新目录。你可以在`store/latest`中看到最新的一次运行结果。

```bash
$ ls store/latest/
history.txt  jepsen.log  results.edn  test.fressian
```

`history.txt`展示了测试执行的操作。不过此处运行完的结果是空的，因为noop测试不会执行任何操作。`jepsen.log`文件拥有一份测试输出到控制台日志的拷贝。`results.edn`展示了对测试的简要分析，也就是每次运行结束我们所看到的输出结果。最后，`test.fressian`拥有测试的原始数据，包括完整的机器可读的历史和分析，如果有需要可以对其进行事后分析。

Jepsen还带有内置的Web浏览器，用于浏览这些结果。 让我们将其添加到我们的`main`函数中：

```clj
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
                   (cli/serve-cmd))
            args))
```

上述代码之所以可以发挥作用，是因为`cli/run!`将命令名称与命令处理器做了映射。我们将这些映射关系用`merge`来合并。

```bash
$ lein run serve
13:29:21.425 [main] INFO  jepsen.web - Web server running.
13:29:21.428 [main] INFO  jepsen.cli - Listening on http://0.0.0.0:8080/
```

我们可以在网络浏览器中打开`http://localhost:8080`来探究我们的测试结果。当然，serve命令带有其自己的选项和帮助信息：

```bash
$ lein run serve --help
Usage: lein run -- serve [OPTIONS ...]

  -h, --help                  Print out this message and exit
  -b, --host HOST    0.0.0.0  Hostname to bind to
  -p, --port NUMBER  8080     Port number to bind to
```

打开一个新的终端窗口，并在其中一直运行Web服务器。 那样我们可以看到测试结果，而无需反复启动和关闭服务器。

有了这个基础之后，我们将编写代码来[设置和拆除数据库](02-cn-db.md)。
