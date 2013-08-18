(ns jepsen.util
  "Kitchen sink")

(def logger (agent nil))
(defn log-print
      [_ & things]
      (apply println things))
(defn log
      [& things]
      (apply send-off logger log-print things))
