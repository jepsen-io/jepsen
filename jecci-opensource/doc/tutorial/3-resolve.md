# Settings
To start writing tests for your cool system, first you need to do some configuration to let jecci correctly route its interfaces to your implementation.

To do this, first change `system` in `settings.clj`, set it to the correct database name, replacing `postgres` to `YOURDBMS`:
```
(def system "postgres")
```
After that, **create a directory in `src/jecci`** with the name you just put. This will be the place where you work on your implementations, every file under this folder should start with namespace as `(ns jecci.YOURSYSNAME.FILENAME)`

Some other settings here are for jdbc, a java library that can connect to DBMS, but as is commented there, they do not fit for all distributed systems, so possibly it will change in the future. 

For now, let's assume you are writing tests for a DBMS, then you should configure settings here, especially 
```
(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:dbtype          "postgresql"
   :dbname          "postgres"
   :user            "jecci"
   :password        "123456"
   :host            (name node)
   :connectTimeout  connect-timeout
   :socketTimeout   socket-timeout})
```

Here `conn-spec` is actually a function, it take node as parameter(wrapped by []), and returns a dictionary. Symbols start with `:` are called keywords, you can treat them as constant, but cost less space in memory. In clojure space and newline is the general seperator, not `,`. If you see `,` in the code, in most cases these are just hints of separation.

To find out the correct configuration for your DBMS, first google your DBMS name and jdbc, hopefully there is some available candidate. You can also try to find it in [maven repo](https://mvnrepository.com/), just search the name of your DBMS, and put it into project.clj, like `[mysql/mysql-connector-java "8.0.22"]`

# Resolver
Now lets look at `src/jecci/interface`.

Here what's being done in `client.clj`, `db.clj` and `nemesis.clj` is that variables will be defined in these namespaces which will be used by codes in `common/`, and the variable will try to read your implementation. The `resolve!` function in `resolver.clj` takes 2 params, first the sub-namespace, second the actual variable. For example `(r/resolve! "bank" "gen-BankClient")` will try to read the variable `jecci.YOURDBMS.bank/gen-BankClient`, where `YOURDBMS` is the one you define in settings.clj.