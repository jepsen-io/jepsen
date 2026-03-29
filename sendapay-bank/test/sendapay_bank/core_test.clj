(ns sendapay-bank.core-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [sendapay-bank.core :as sut]))

(defn- write-temp-file!
  [content]
  (let [file (java.io.File/createTempFile "sendapay-bank-core-test" ".tsv")]
    (.deleteOnExit file)
    (spit file content)
    (.getCanonicalPath file)))

(deftest parse-pg-stat-activity-snapshot-test
  (let [path (write-temp-file!
               (str "pid\tusename\tapplication_name\tclient_addr\tstate\twait_event_type\twait_event\n"
                    "100\tsendapay\thelper\t10.0.0.1\tactive\tLock\ttransactionid\n"
                    "101\tsendapay\thelper\t10.0.0.2\tidle\tClient\tClientRead\n"
                    "102\tsendapay\thelper\t10.0.0.3\tactive\tLWLock\tbuffer_content\n"))
        metadata {:category "db"
                  :node "n2"
                  :kind :pg-stat-activity
                  :capture-id "123-after-start-db"
                  :capture-label "after-start-db"
                  :file-name "123-after-start-db-pg-stat-activity.tsv"
                  :timestamp-ms 123}
        entry (#'sut/parse-pg-stat-activity-snapshot metadata path)]
    (is (= :ok (:capture-status entry)))
    (is (= 3 (:session-count entry)))
    (is (= 2 (:active-session-count entry)))
    (is (= 2 (:waiting-session-count entry)))
    (is (= 1 (:blocked-session-count entry)))))

(deftest parse-pg-locks-snapshot-test
  (let [path (write-temp-file!
               (str "pid\tgranted\tlocktype\tmode\n"
                    "100\tt\trelation\tRowExclusiveLock\n"
                    "200\tf\trelation\tAccessExclusiveLock\n"
                    "200\t0\ttransactionid\tShareLock\n"
                    "201\tfalse\tvirtualxid\tExclusiveLock\n"))
        metadata {:category "db"
                  :node "n2"
                  :kind :pg-locks
                  :capture-id "123-after-start-db"
                  :capture-label "after-start-db"
                  :file-name "123-after-start-db-pg-locks.tsv"
                  :timestamp-ms 123}
        entry (#'sut/parse-pg-locks-snapshot metadata path)]
    (is (= :ok (:capture-status entry)))
    (is (= 4 (:total-lock-count entry)))
    (is (= 1 (:granted-lock-count entry)))
    (is (= 3 (:ungranted-lock-count entry)))
    (is (= 2 (:blocked-session-count entry)))))

(deftest parse-db-socket-snapshot-test
  (let [header (str/join "\t" (map name @#'sut/db-socket-summary-columns))
        row (str/join "\t" ["ok" "after-stop-db" "n1" "5" "3" "1" "0" "0" "1" "0" "0" "0" "0" "0" "0"])
        path (write-temp-file! (str header "\n" row "\n"))
        metadata {:category "app"
                  :node "n1"
                  :kind :db-sockets
                  :capture-id "456-after-stop-db"
                  :capture-label "after-stop-db"
                  :file-name "456-after-stop-db-db-sockets.tsv"
                  :timestamp-ms 456}
        entry (#'sut/parse-db-socket-snapshot metadata path)]
    (is (= :ok (:capture-status entry)))
    (is (= "ok" (:status entry)))
    (is (= 5 (:total_connection_count entry)))
    (is (= 3 (:established_count entry)))
    (is (= 1 (:syn_sent_count entry)))
    (is (= 1 (:close_wait_count entry)))
    (is (= 0 (:other_count entry)))))

(deftest summarize-db-socket-snapshots-test
  (let [entries [{:capture-status :ok
                  :total_connection_count 2
                  :established_count 1
                  :syn_sent_count 0
                  :close_wait_count 0
                  :time_wait_count 0
                  :other_count 0}
                 {:capture-status :error
                  :total-connection-count 6
                  :established-count 4
                  :syn-sent-count 2
                  :close-wait-count 3
                  :time-wait-count 5
                  :other-count 7}]
        summary (#'sut/summarize-db-socket-snapshots entries)]
    (is (= 2 (:count summary)))
    (is (= 1 (:error-count summary)))
    (is (= 6 (:max-total-connection-count summary)))
    (is (= 4 (:max-established-count summary)))
    (is (= 2 (:max-syn-sent-count summary)))
    (is (= 3 (:max-close-wait-count summary)))
    (is (= 5 (:max-time-wait-count summary)))
    (is (= 7 (:max-other-count summary)))))
