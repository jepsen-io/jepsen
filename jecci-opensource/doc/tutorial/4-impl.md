# db Implementation
As is described in intro, you need to implement db.clj in interface first to let jepsen know how to setup your process.

First **create a file `src/jecci/YOURDBMS/db.clj`**, you are gonna put your implementation here. Let's look at interface/db.clj, where there is only one required implementation:
```
(def db (r/resolve! "db" "db"))
```

It is a quite complicated one, see `postgres/db.clj` to learn more about how to use it, the code should be easy to read. 

Some important functions including:

- `jepsen.control/exec`
run a command on the node, and return the stdout, requires `jecci.utils.handy/exec->info` or etc. to print the result if necessary. If need to pass strings quoted inside strings, like `"\"sth\""`, then you will need to use `jepsen.control/lit`, like `(jepsen.control/exec "echo" (jepsen.control/lit "\"sth\""))`

- `jepsen.control/su`
switch to root

- `jepsen/synchronize`
let currently running threads wait until every thread execute to this line. Need an object as the monitor.

- `locking`
Requires an object to be monitor, can be any kinds of objects. Functions as acquiring a mutex.

Note that at the very begining of `postgres/db.clj`, it is importing `[jepsen.control :as c]`, so if there is such replacement, `c` will represents `jepsen.control` in this namespace.

# Client Implementation
Jecci has lots of pre-defined tests, borrowing from TiDB, and it only takes very little effort to use them. Each test needs a corresponding client implementation, but in jecci there are templates for each test, all you need to do is to provide the translation for your database. See `interface/clients.clj`, these are all tests with templates to utilize:
```
gen-BankClient 
gen-MultiBankClient 
gen-IncrementClient 
gen-AtomicClient 
gen-SequentialClient 
gen-SetClient 
gen-CasSetClient 
gen-TableClient 
gen-Client 
```

Let's take bank as an example. First **create the file `src/jecci/YOURDBMS/bank.clj`**. Put the correct namespace and some requirements here, for example in `postgres/bank.clj`:
```
(ns jecci.postgres.bank
  (:require [jecci.utils.dbms.bank :as b]
            [jecci.postgres.db :as db]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))
```
Here `[jecci.utils.dbms.bank :as b]` contains the templates for bank clients. 

To use it, first create a record `pg-BankClient`. A record is like a struct in c, variables wrapped by [] is the members of the struct. To create a variable with the type of the struct, do `(pg-BankClient. nil)`. Note that '.' here means create a new pg-BankClient struct variable. nil is like None or NULL in other language.

Here we are implementing `client/Client`, which is a protocol. The protocol includes 5 functions, open!, setup!, invoke!, teardown!, close!. We are not implementing the actual functions for bank client here, but a simple wrapper for the real client, it filters only master do the setup and synchronize threads after the leaders have done setting up, like create table, etc. It also filters followers from doing write operations, because in pg only leader can do write operations.
```
(defrecord pg-BankClient [dbclient]
  client/Client
  (open! [this test node]
    ; open! needs to return a client whose conn has been initialized
    ; and note that here we are returning a pg-BankClient, NOT dbclient!
    (pg-BankClient. (client/open! dbclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn dbclient)))
      (client/setup! dbclient test)) 
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (or (db/isleader? (:node# (:conn dbclient)))
         (= (:f op) :read))
      (client/invoke! dbclient test op) 
     (throw (Exception. "not writing to backup")))
    )
  (teardown! [this test]
    (client/teardown! dbclient test))
  (close! [this test]
    (client/close! dbclient test)))
```

And this function will create a struct and return it:
```
(defn gen-BankClient [conn]
  (pg-BankClient. (b/gen-BankClient conn BankClientTranslation)))
```

Note that `b/gen-BankClient` is the actual function that will read the `conn` and `BankClientTranslation` params and return a `BankClient` that does connection, sql, etc. Here `BankClientTranslation` is a dictionary that translate operations, which are keywords, into sql statements for postgresql. And, if there are variables, the value of the corresponding key in the dict will be a function, taking the params and translate it into sql statement.
```
(def BankClientTranslation
  {
   :create-table ["create table if not exists accounts
                  (id int not null primary key,
                  balance bigint not null)"]
   :read-all [(str "select * from accounts")]
   :read-from (fn [read-lock from]
                [(str "select * from accounts where id=? " read-lock) from])
   :read-to (fn [read-lock to]
              [(str "select * from accounts where id = ? " read-lock) to])
   :update-from (fn [amount from]
                  ["update accounts set balance = balance - ? where id = ?" amount from])
   :update-to (fn [amount to]
                ["update accounts set balance = balance + ? where id = ?" amount to])
   })
```

Now let's try it out, with the following command:
```
lein run test -w bank --nemesis none --time-limit 20 --test-count 1 --concurrency 2n --username jecci --password 123456 --pure-generators
```

For the rest of clients, you can just do the same. Note that `gen-Client` is a bit special, see `postgres/txn.clj`, it requires you to first create a `mop!` function.

# Nemesis Implementation
See `interface/nemesis.clj`. Here contains all interfaces you need to implement. Please see the example `postgres/nemesis.clj` to learn more about how to implement them. By default there are some network partition nemeses, you have to implement others if you need.

### `plot-spec`
Should be a dictionary, which specifies how each nemesis is ploted in the graph.

### `nemesis-specs`
Should be a set. Note that `#{:arg1 :arg2}` means a set containing `:arg1` and `:arg2`. Put all nemeses that you implement here.

### `all-nemeses`, `quick-nemeses` and `very-quick-nemeses`
This three is used when running jecci by `lein run test-all`. They should be vectors in the form of `[[] [:partition] [:kill-sth] [:partition :kill-sth]]`. During running `test-all`, it will run all combinations of workloads and nemeses, like for example, it will first run bank test without nemeses, then bank test with partition nemesis, then bank test with kill-sth nemeses, then a nemesis that each time it tries to destruct the system, it will randomly choose one from partition and kill-sth.

### `nemesis-composition`
Should be a dict. The keys can be set or dict, and the value should be a function that reify jepsen.nemesis/Nemesis. reify creates an anonymous class, and by reify the Nemesis, jepsen can call the functions you implement.

But what if the key is a dict? For example {:kill-postmaster :kill}, well remember that operations in jepsen is like `{:type :invoke, :op :read,  :f YOURFUNCIMPL, :value nil}`, same for nemesis, for `:kill-postmaster`, the nemesis will be like `{:type :invoke, :op :kill-postmaster, :value nil}`, and when routing the operation to the function(the one you reify) that takes the operation, it will change the `:op` from `:kill-postmaster` to `:kill`.

### `op2heal-dict`
Should be a dict, which defines the recovery op of a nemesis.

### `autorecover-ops`
Unfinished yet. For some dbms the nemesis is self-healed, which behaves different from other kinds of nemeses, so it makes the plotting weird. But anyway, nemeses put here only has a keyword as the value that serves as a stop symbol for plotting.

### `final-gen-dict`
Should be a dict, will be execute at the end of a test.

### `special-full-generator`
Should be a function that takes a keyword as arg, and return a generator that gives jepsen nemesis operations to execute. If defined, will use this generator only, otherwise will use the ones defined in op2heal-dict and autorecover-ops. You dont have to implement it, by default it's nil