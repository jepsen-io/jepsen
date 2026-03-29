(ns sendapay-bank.summary
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :refer [parse-opts]]))

(def ^:private cli-options
  [[nil "--mode MODE" "Result set to inspect"
    :default "classic"
    :validate [#(#{"classic" "append-only"} %) "must be classic or append-only"]]
   [nil "--path PATH" "Explicit results.edn path or run directory"]
   [nil "--latest" "Read the latest run for the selected mode"]
   ["-h" "--help" "Show help"]])

(defn- project-root []
  (.getCanonicalPath (io/file (System/getProperty "user.dir"))))

(defn- mode->test-name
  [mode]
  (case mode
    "append-only" "sendapay-bank-append-only"
    "classic" "sendapay-bank-classic"))

(defn- usage
  [summary]
  (str "Usage: lein run -m sendapay-bank.summary [options]\n\nOptions:\n"
       summary))

(defn- results-file?
  [file]
  (and (.isFile file)
       (= "results.edn" (.getName file))))

(defn- latest-results-file
  [mode]
  (let [store-root (io/file (project-root) "store" (mode->test-name mode))]
    (when (.exists store-root)
      (->> (file-seq store-root)
           (filter results-file?)
           (sort-by (juxt #(.lastModified ^java.io.File %)
                          #(.getCanonicalPath ^java.io.File %)))
           last))))

(defn- resolve-results-file
  [{:keys [mode path latest]}]
  (cond
    path
    (let [candidate (io/file path)
          results-file (if (.isDirectory candidate)
                         (io/file candidate "results.edn")
                         candidate)]
      (when-not (results-file? results-file)
        (throw (ex-info "results.edn not found"
                        {:path path})))
      (.getCanonicalFile results-file))

    latest
    (or (latest-results-file mode)
        (throw (ex-info "no results.edn files found for mode"
                        {:mode mode})))

    :else
    (or (latest-results-file mode)
        (throw (ex-info "no results.edn files found for mode"
                        {:mode mode})))))

(defn- read-edn-file
  [path]
  (with-open [reader (java.io.PushbackReader. (io/reader path))]
    (edn/read reader)))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      (do
        (println (usage summary))
        (System/exit 0))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [error errors]
            (println error))
          (println)
          (println (usage summary)))
        (System/exit 2))

      :else
      (try
        (let [results-file (resolve-results-file options)
              results (read-edn-file results-file)
              nemesis-summary (:nemesis-summary results)]
          (when-not (map? nemesis-summary)
            (throw (ex-info "results.edn does not contain :nemesis-summary"
                            {:path (.getCanonicalPath results-file)})))
          (println (.getCanonicalPath results-file))
          (pprint nemesis-summary))
        (catch Exception e
          (binding [*out* *err*]
            (println (.getMessage e)))
          (System/exit 1))))))
