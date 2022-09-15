(ns jepsen.lazyfs-test
  "Tests for the lazyfs write-losing filesystem"
  (:require [clojure [data :refer [diff]]
                     [pprint :refer [pprint]]
                     [string :as str]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as g]
                                [properties :as prop]
                                [results :refer [Result]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [generator :as gen]
                    [lazyfs :as lazyfs]
                    [nemesis :as nem]
                    [os :as os]
                    [tests :as tests]
                    [util :as util :refer [pprint-str
                                           timeout]]]
            [jepsen.control [core :as cc]
                            [util :as cu]
                            [sshj :as sshj]]
            [jepsen.os.debian :as debian]
            [knossos.op :as op]
            [slingshot.slingshot :refer [try+ throw+]]))

; (use-fixtures :once quiet-logging)

(defrecord FileSetClient [dir file node]
  client/Client
  (open! [this test node]
    (assoc this
           :file (str dir "/set")
           :node node))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (-> (c/on-nodes test [node]
                    (fn [_ _]
                      (case (:f op)
                        :add  (do (c/exec :echo (str (:value op) " ") :>> file)
                                  (assoc op :type :ok))
                        :read (let [vals (-> (c/exec :cat file)
                                             (str/split #"\s+")
                                             (->> (remove #{""})
                                                  (mapv parse-long)))]
                                (assoc op :type :ok, :value vals)))))
        (get node)))

  (teardown! [this test])

  (close! [this test]))

(defn file-set-client
  "Writes a set to a single file on one node, in the given directory."
  [dir]
  (map->FileSetClient {:dir dir}))

(deftest ^:integration file-set-test
  (let [dir    "/tmp/jepsen/file-set-test"
        lazyfs (lazyfs/lazyfs dir)
        test (assoc tests/noop-test
                    :name      "lazyfs file set"
                    :os        debian/os
                    :db        (lazyfs/db lazyfs)
                    :client    (file-set-client dir)
                    :nemesis   (lazyfs/nemesis lazyfs)
                    :generator (gen/phases
                                 (->> (range)
                                      (map (fn [x] {:f :add, :value x}))
                                      (gen/delay 1/100)
                                      (gen/nemesis
                                        (->> {:type :info
                                              :f    :lose-unfsynced-writes
                                              :value ["n1"]}
                                             repeat
                                             (gen/delay 1/2)))
                                      (gen/time-limit 5))
                                 (gen/clients {:f :read}))
                    :checker   (checker/set)
                    :nodes     ["n1"])
        test (jepsen/run! test)]
    ;(pprint (:history test))
    ;(pprint (:results test))
    (is (false? (:valid? (:results test))))
    (is (pos? (:ok-count (:results test))))
    (is (pos? (:lost-count (:results test))))))

;; More general FS tests

(defn trace-remote
  "A jepsen.remote/Remote which also logs every command to an atom containing a
  vector."
  [log remote]
  (reify cc/Remote
    (connect [this conn-spec]
      (trace-remote log (cc/connect remote conn-spec)))

    (disconnect! [this]
      (cc/disconnect! remote))

    (execute! [this context action]
      (try
        (let [res (cc/execute! remote context action)]
          (swap! log conj res)
          res)
        (catch Throwable t
          (swap! log conj (assoc action :exception t))
          (throw t))))

    (upload! [this context local-paths remote-path opts]
      (cc/upload! remote context local-paths remote-path opts))

    (download! [this context remote-paths local-path opts]
      (cc/download! remote context remote-paths local-path opts))))

(defn split-path
  "Splits a path string on / separators."
  [path-str]
  (str/split #"/" path-str))

(defn join-path
  "Joins a seqable of strings with / into a single string."
  [path-fragments]
  (str/join "/" path-fragments))

(defn parent-dir
  "Given a path as a vector of strings, returns the parent directory of that
  path. Nil if the path has no directories."
  [path]
  (butlast path))

(defn model-dir
  "Constructs a model filesystem state. Our state is a map of file/directory
  names to maps representing those files or directories. Directories are:

    {:type  :dir
     :files {... another fs map ...}}

  Files are:

    {:type :file
     :data \"a string\"}"
  []
  {:type :dir
   :files {}})

(defn model-file
  "Constructs an empty model of a file."
  []
  {:type :file
   :data ""})

(defn model-dir?
  "Is this a model directory?"
  [dir]
  (= :dir (:type dir)))

(defn model-file?
  "Is this a model file?"
  [file]
  (= :file (:type file)))

(defn assert-file
  "Throws {:type ::not-file} when x is not a file. Returns file."
  [file]
  (when-not (model-file? file)
    (throw+ {:type ::not-file}))
  file)

(defn assert-dir
  "Throws {:type ::not-dir} when x is not a directory. Returns dir."
  [dir]
  (when-not (model-dir? dir)
    (throw+ {:type ::not-dir}))
  dir)

(defn get-path*
  "Takes a root directory map and a path (as a vector of strings). Returns the
  file or object at that position, or nil iff it doesn't exist. The empty path
  refers to the root. Throws:

    {:type ::not-dir}  When some directory in path isn't a directory."
  [root path]
  (when root
    (if (seq path)
      (do (assert-dir root)
          (let [[filename & more] path
                file (get (:files root) filename)]
            (if (seq more)
              ; Descend
              (get-path* file more)
              ; We got it
              file)))
      ; We're asking for the root itself
      root)))

(defn get-path
  "Like get-path*, but throws :type ::does-not-exist when the thing doesn't
  exist."
  [root path]
  (if-let [entry (get-path* root path)]
    entry
    (throw+ {:type ::does-not-exist})))

(defn assoc-path
  "Takes a root directory map and a path (as a vector of strings) and a new
  file/dir entry. The empty path refers to the root. Returns the root with that
  path set to that file/dir. Assoc'ing nil deletes that path.

  Throws:

    {:type ::not-dir}        When some directory in path isn't a directory.
    {:type ::does-not-exist} When some directory in path doesn't exist."
  [root path entry']
  (when-not root
    (throw+ {:type ::does-not-exist}))
  (assert-dir root)
  (if (seq path)
    (let [[filename & more] path
          entry (get (:files root) filename)]
      (if more
        ; Descend
        (assoc-in root [:files filename] (assoc-path entry more entry'))
        ; This is the final directory
        (if (nil? entry')
          ; Delete
          (update root :files dissoc filename)
          ; Update
          (assoc-in root [:files filename] entry'))))
    ; Replace this root
    entry'))

(defn update-path*
  "Takes a root directory map and a path (as a vector of strings) and a
  function that transforms whatever file/dir entry is at that path. The empty
  path refers to the root. If the thing referred to by the path doesn't exist,
  passes `nil` to transform. To delete an entry, return nil from transform.

  Throws:

    {:type ::not-dir}        When some directory in path isn't a directory.
    {:type ::does-not-exist} When some directory in path doesn't exist."
  [root path transform]
  (let [file  (get-path* root path)
        file' (transform file)]
    (assoc-path root path file')))

(defn update-dir
  "Takes a root directory map, a path (as a vector of strings), and a function
  which takes a directory map and modifies it somehow. Descends into the given
  path, applies f to it, and returns the modified root directory.

  Throws:

    {:type ::not-dir}        When some path component isn't a directory.
    {:type ::does-not-exist} When the path doesn't exist."
  [root path transform]
  (update-path* root path (fn [dir]
                            (when-not dir
                              (throw+ {:type ::does-not-exist}))
                            (assert-dir dir)
                            (transform dir))))

(defn update-file*
  "Takes a root directory map, a path to a file (a vector of strings), and a
  function which takes that file and modifies it. Descends into that directory,
  applies f to the file, and returns the new root.

  This variant allows non-existent files to be modified: transform will receive
  `nil` if the file doesn't exist, but its directory does.

  Throws everything update-dir does, plus

    {:type ::not-file}  If the path exists but isn't a file"
  [root path transform]
  (update-path* root path (fn [file]
                            (when (and (not (nil? file))
                                       (not= :file (:type file)))
                              (throw+ {:type ::not-file}))
                            (transform file))))

(defn update-file
  "Takes a root directory map, a path to a file (a vector of strings), and a
  function which takes that file and modifies it. Descends into that directory,
  applies f to the file, and returns the new root.

  Throws everything update-dir does, plus

  {:type ::does-not-exist}  If the file doesn't exist
  {:type ::not-file}        If the path is not a file"
  [root path transform]
  (update-file* root path
                (fn [file]
                  (when-not file
                    (throw+ {:type ::does-not-exist}))
                  (transform file))))

(defn dissoc-path
  "Takes a root directory map and a path (as a vector of strings). Deletes that
  path and returns root. Throws:

    {:type ::not-dir}             When some directory in path isn't a directory
    {:type ::does-not-exist}      When some part of the path doesn't exist
    {:type ::cannot-dissoc-root}  When trying to dissoc the root"
  [root path]
  (if (seq path)
    (update-path* root path (fn [entry]
                              (if entry
                                nil
                                (throw+ {:type ::does-not-exist}))))
    (throw+ {:type ::cannot-dissoc-root})))

(defn model-fs-op
  "Applies a filesystem operation to a simulated in-memory filesystem state.
  Returns a pair of [state', op']: the resulting state, and the completion
  operation."
  [root {:keys [f value] :as op}]
  (try+
    (case f
      :append
      (try+
        (let [[path data] value]
          [(update-file* root path
                         (fn [file]
                           (let [file (or file (model-file))]
                             (update file :data str data))))
           (assoc op :type :ok)]))

      :mkdir
      [(update-path* root value
                     (fn [path]
                       ; We expect this to be empty
                       (when-not (nil? path)
                         (throw+ {:type ::exists}))
                       (model-dir)))
       (assoc op :type :ok)]


      :mv (try+
            (let [[from-path to-path] value
                  from-entry (get-path* root from-path)
                  to-entry   (get-path* root to-path)
                  ; If moving to a directory, put us *inside* the given dir
                  to-path    (if (model-dir? to-entry)
                               (vec (concat to-path [(last from-path)]))
                               to-path)
                  to-entry   (get-path* root to-path)]

              ; Look at ALL THESE WAYS TO FAIL
              (assert-dir (get-path* root (parent-dir to-path)))

              (when (nil? from-entry)
                (throw+ {:type ::does-not-exist}))

              (when (= from-path to-path)
                (throw+ {:type ::same-file}))

              (when (and (model-dir? to-entry)
                         (not (model-dir? from-entry)))
                (throw+ {:type ::cannot-overwrite-dir-with-non-dir}))

              (when (and to-entry
                         (model-dir? from-entry)
                         (not (model-dir? to-entry)))
                (throw+ {:type ::cannot-overwrite-non-dir-with-dir}))

              (when (= from-path (take (count from-path) to-path))
                (throw+ {:type ::cannot-move-inside-self}))

              ; When moving a directory on top of another directory, the target
              ; must be empty.
              (when (and (model-dir? from-entry)
                         (model-dir? to-entry)
                         (seq (:files to-entry)))
                (throw+ {:type ::not-empty}))

              [(-> root
                   (dissoc-path from-path)
                   (assoc-path to-path from-entry))
               (assoc op :type :ok)])
            ; Working out the exact order that mv checks and throws errors
            ; has proven incredibly hard so we collapse some of them to
            ; one value.
            (catch [:type ::not-dir] _
              (throw+ {:type ::does-not-exist})))

      :read
      (let [path (first value)
            entry (get-path root path)]
        (when (not= :file (:type entry))
          (throw+ {:type ::not-file}))
        [root (assoc op :type :ok, :value [path (:data entry)])])

      :rm
      (do (get-path root value) ; Throws if does not exist
          [(dissoc-path root value) (assoc op :type :ok)])

      :touch
      [(update-dir root (parent-dir value)
                   (fn [dir]
                     (cond ; Already exists
                           (contains? (:files dir) (peek value))
                           dir

                           ; Doesn't exist
                           true
                           (assoc-in dir [:files (peek value)] (model-file)))))
       (assoc op :type :ok)]

      :write
      (let [[path data] value
            filename    (peek path)
            root' (update-dir root (parent-dir path)
                              (fn [dir]
                                (let [file (get (:files dir) filename (model-file))]
                                  (when-not (= :file (:type file))
                                    (throw+ {:type ::not-file}))
                                  (let [file' (assoc file :data data)]
                                    (assoc-in dir [:files filename] file')))))]
        [root' (assoc op :type :ok)]))
    (catch [:type ::cannot-move-inside-self] e
      [root (assoc op :type :fail, :error :cannot-move-inside-self)])
    (catch [:type ::cannot-overwrite-dir-with-non-dir] e
      [root (assoc op :type :fail, :error :cannot-overwrite-dir-with-non-dir)])
    (catch [:type ::cannot-overwrite-non-dir-with-dir] e
      [root (assoc op :type :fail, :error :cannot-overwrite-non-dir-with-dir)])
    (catch [:type ::does-not-exist] e
      [root (assoc op :type :fail, :error :does-not-exist)])
    (catch [:type ::exists] e
      [root (assoc op :type :fail, :error :exists)])
    (catch [:type ::not-dir] e
      [root (assoc op :type :fail, :error :not-dir)])
    (catch [:type ::not-empty] e
      [root (assoc op :type :fail, :error :not-empty)])
    (catch [:type ::not-file] e
      [root (assoc op :type :fail, :error :not-file)])
    (catch [:type ::same-file] e
      [root (assoc op :type :fail, :error :same-file)])))

(defn fs-op!
  "Applies a filesystem operation to the local node."
  [{:keys [f value] :as op}]
  (case f
    :append
    (try+ (let [[path data] value
                cmd (->> [:cat :>> (join-path path)]
                         (map c/escape)
                         (str/join " "))
                action {:cmd cmd
                        :in data}]
            (-> action
                c/wrap-cd
                c/wrap-sudo
                c/wrap-trace
                c/ssh*
                cc/throw-on-nonzero-exit))
          (assoc op :type :ok)
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Is a directory"  :not-file
                            #"Not a directory" :not-dir
                            #"No such file"    :does-not-exist

                            (throw+ e)))))

    :mkdir
    (try+ (c/exec :mkdir (join-path value))
          (assoc op :type :ok)
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"File exists"               :exists
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist
                            (throw+ e)))))

    :mv
    (try+ (let [[from to] value]
            (c/exec :mv (join-path from) (join-path to))
            (assoc op :type :ok))
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"are the same file"         :same-file
                            #"cannot move .+ to a subdirectory of itself"
                            :cannot-move-inside-self
                            #"cannot overwrite directory .+ with non-directory"
                            :cannot-overwrite-dir-with-non-dir
                            #"cannot overwrite non-directory .+ with directory"
                            :cannot-overwrite-non-dir-with-dir
                            #"Directory not empty"
                            :not-empty
                            #"Not a directory"           ;:not-dir
                            :does-not-exist ; We collapse these for convenience
                            #"No such file or directory" :does-not-exist

                            (throw+ e)))))

    :read
    (try+ (let [[path] value
                data   (c/exec :cat (join-path path))]
            (assoc op :type :ok, :value [path data]))
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Is a directory"            :not-file
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist

                            (throw+ e)))))

    :rm (try+ (do (c/exec :rm :-r (join-path value))
                  (assoc op :type :ok))
              (catch [:exit 1] e
                (assoc op
                       :type :fail
                       :error (condp re-find (:err e)
                                #"Not a directory"           :not-dir
                                #"No such file or directory" :does-not-exist
                                (throw+ e)))))

    :touch
    (try+ (c/exec :touch (join-path value))
          (assoc op :type :ok)
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist
                            (throw+ e)))))


    :write (let [[path data] value]
             (try+ (cu/write-file! data (join-path path))
                   (assoc op :type :ok)
                   (catch [:exit 1] e
                     (assoc op
                            :type :fail
                            :error (condp re-find (:err e)
                                     #"Is a directory"  :not-file
                                     #"Not a directory" :not-dir
                                     #"No such file"    :does-not-exist

                                     (throw+ e))))))))

(defrecord FSClient [dir node]
  client/Client
  (open! [this test node]
    (assoc this :node node))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (timeout 10000 (assoc op :type :info, :error :timeout)
             (-> (c/on-nodes test [node]
                             (fn [_ _]
                               ;(c/trace
                               (c/cd dir (fs-op! op))))
                 (get node))))

  (teardown! [this test])

  (close! [this test]))

(defn fs-client
  "Constructs a client which performs filesystem ops in the given directory."
  [dir]
  (map->FSClient {:dir dir}))

(defn simple-trace
  "Takes a collection of ops from a history and strips them down to just [type
  f value maybe-error] tuples, to make it easier to read traces from
  fs-checker."
  [history]
  (->> history
       (mapv (fn [{:keys [type f value error]}]
               (if error
                 [type f value error]
                 [type f value])))))

(defn fs-checker
  "A checker which compares the actual history to a simulated model
  filesystem."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; Simulate evolution
            [roots
             model-history]
            (loopr [root     (model-dir)
                    invoke   nil
                    roots    []
                    history' []]
                   [op history]
                   (cond (= :nemesis (:process op))
                         (recur root
                                invoke
                                (conj roots root)
                                (conj history' op))

                         (= :invoke (:type op))
                         (recur root
                                op
                                (conj roots root)
                                (conj history' op))

                         true
                         (let [[root' op'] (model-fs-op root invoke)
                               ; We want to use the index and timing info from
                               ; the actual completion
                               op' (assoc op'
                                          :time (:time op)
                                          :index (:index op))]
                           (recur root'
                                  nil
                                  (conj roots root')
                                  (conj history' op'))))
                   [roots history'])
            ;_ (info :history (pprint-str history))
            ;_ (info :model-history (pprint-str model-history))
            ; Look for earliest divergence
            divergence (loop [i 0]
                         (if (<= (count history) i)
                           ; Done!
                           nil
                           (let [actual   (nth history i)
                                 expected (nth model-history i)]
                             (if (= actual expected)
                               ; Fine
                               (recur (inc i))
                               ; Not fine
                               (let [; Compute a window of indices
                                     is (range 0 #_(max 0 (- i 10)) (inc i))
                                     ; And take those recent ops...
                                     ops (->> (map history is)
                                              (remove op/invoke?))
                                     ; And the state just prior to i, and after
                                     ; applying i
                                     root  (if (pos? i)
                                             (nth roots (dec i))
                                             (model-dir))
                                     root' (nth roots i)]
                                 {:index    i
                                  :trace    (simple-trace ops)
                                  :root     root
                                  :expected expected
                                  :actual   actual})))))]
        (if divergence
          (assoc divergence
                 :valid? false)
          {:valid? true})))))

(defn print-remote-log-op
  "Prints an operation from the remote log."
  [fs-op-pattern op]
  (let [{:keys [cmd in out err exit exception]} op
        cmd              (str/replace cmd fs-op-pattern "")]
    (println
      (str cmd
           (when in           (str " <<< " (c/escape in)))
           " # => "
           (when-not (str/blank? out) (pr-str out))
           (when-not (str/blank? err) (str "(stderr: " (pr-str err) ")"))
           (when exception    (str " Threw " exception))))))

(defn print-remote-log
  "Prints out a remote log as a shell script which reproduces an issue."
  [dir results remote-log]
  (let [; All fs ops start with this:
        fs-op-pattern #"^cd /tmp/jepsen/fs-test; "
        ; Finds out if something is an fs op
        fs-op? (fn [action]
                 (re-find fs-op-pattern (:cmd action)))
        ; Break up log
        [preamble rest-log] (split-with (complement fs-op?) remote-log)
        ;_ (prn :preamble)
        ;_ (pprint preamble)
        ; Get the lazyfs setup commands
        lazyfs-setup (drop-while
                       (fn [action]
                         (not (re-find #"mkdir -p /tmp/jepsen/fs-test"
                                       (:cmd action))))
                       preamble)
        lazyfs-setup (take-while
                       (fn [action]
                         (not (re-find #"findmnt" (:cmd action))))
                       lazyfs-setup)
        ; And the fs operations
        fs-log              (take-while fs-op? rest-log)
        _                   (println "Log length: " (count fs-log))
        ; Extract only the fs ops that led to the failure
        fs-log              (take (/ (inc (:index results)) 2) fs-log)]

    (mapv (partial print-remote-log-op fs-op-pattern) lazyfs-setup)
    (println)
    (println "cd /tmp/jepsen/fs-test")
    (mapv (partial print-remote-log-op fs-op-pattern) fs-log)))

(defn print-fs-test
  "Prints out a (presumably failing) fs test to the filesystem. Returns the
  path to that file."
  [test]
  (let [file (str "/tmp/jepsen/fs-test-" (rand-int 10000000))]
    (with-open [w (io/writer file)]
      (binding [*out* w]
        (let [dir (:dir test)
              res (:results test)]
          ;(pprint res)
          (print-remote-log dir res (:remote-log test))
          (println)
          (println "At this point, the root was theoretically")
          (pprint (:root res))
          (println)
          (println "And we expected to execute")
          (pprint (:expected res))
          (println)
          (println "But with lazyfs we actually executed")
          (pprint (:actual res)))))
    file))

(defn run-fs-test
  "Runs an entire Jepsen test for lazyfs fs test, given a specific history of
  invocations to evaluate."
  [dir history]
  (let [dir        "/tmp/jepsen/fs-test"
        lazyfs     (lazyfs/lazyfs dir)
        remote-log (atom [])
        remote     (trace-remote remote-log c/ssh)
        test (assoc tests/noop-test
                    :name      "lazyfs fs-test"
                    :dir       dir
                    :remote    remote
                    :os        debian/os
                    :db        (lazyfs/db lazyfs)
                    :client    (fs-client dir)
                    :nemesis   (lazyfs/nemesis lazyfs)
                    :generator (->> history
                                    ;(gen/stagger 1/20)
                                    (gen/nemesis
                                      nil
                                      #_ (->> {:type :info
                                               :f    :lose-unfsynced-writes
                                               :value ["n1"]}
                                           repeat
                                           (gen/stagger 1/2))))
                    :checker   (fs-checker)
                    :nodes     ["n1"])
        test (jepsen/run! test)]
    (assoc test :remote-log @remote-log)))

(def gen-path
  "test.check generator for a random fs-test path. Generates vectors of length
  1 or 2 of \"a\" or \"b\"--we want a real small state space here or we'll
  almost never do anything interesting on the same files."
  (g/vector (g/elements ["a" "b"]) 1 2))

(def data-gen
  "Generates a short string of data to write to a file for an fs test"
  (g/scale #(/ % 100) g/string-alphanumeric))

(def fs-op-gen
  "test.check generator for fs test ops. Generates append, write, mkdir, mv,
  read, etc."
  (g/one-of
    [(g/let [path gen-path]
       {:f :read, :value [path nil]})
     (g/let [path gen-path]
       {:f :touch, :value path})
     (g/let [path gen-path, data data-gen]
       {:f :append, :value [path (str " " data)]})
     (g/let [source gen-path, dest gen-path]
       {:f :mv, :value [source dest]})
     (g/let [path gen-path, data data-gen]
       {:f :write, :value [path data]})
     (g/let [path gen-path]
       {:f :mkdir, :value path})
     (g/let [path gen-path]
       {:f :rm, :value path})]))

(def fs-history-gen
  "Generates a whole history"
  (g/scale (partial * 1000)
    (g/vector fs-op-gen)))

(defspec ^:integration ^:focus fs-spec
  (let [dir "/tmp/jepsen/fs-test"]
    (prop/for-all [history fs-history-gen]
                  (let [test (run-fs-test dir history)
                        ; As a side effect, print out failing cases
                        outfile (when-not (:valid? (:results test))
                                  ;(pprint (:history test))
                                  ;(pprint (:results test))
                                  (print-fs-test test))]
                    (reify Result
                      (pass? [_]
                        (:valid? (:results test)))
                      (result-data [_]
                        {:file outfile}))))))
