(defproject io.jepsen/antithesis "0.1.0-SNAPSHOT"
  :description "Support for running Jepsen inside Antithesis"
  :url "https://github.com/jepsen-io/jepsen"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[io.jepsen/generator "0.1.0-SNAPSHOT"]
                 [com.antithesis/sdk "1.4.4"]]
  :java-source-paths ["src"]
  :javac-options ["--release" "17"]
  :repl-options {:init-ns jepsen.antithesis}
  :test-selectors {:default (fn [m]
                              (not (:perf m)))
                   :focus :focus
                   :perf :perf})
