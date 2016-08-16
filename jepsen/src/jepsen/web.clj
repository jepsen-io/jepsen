(ns jepsen.web
  "Web server frontend for browsing test results."
  (:require [jepsen.store :as store]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [hiccup.core :as h]
            [ring.util.response :as response]
            [org.httpkit.server :as server]))

(defn fast-tests
  "Abbreviated set of tests"
  []
  (->> (store/tests)
       (mapcat (fn [[test-name runs]]
                 (keep (fn [[test-time full-test]]
                         (try
                           {:name        test-name
                            :start-time  test-time
                            :results     (store/load-results test-name test-time)}
                           (catch java.io.FileNotFoundException e
                             ; Incomplete test
                             nil)))
                      runs)))))

(defn test-header
  []
  [:tr
   [:th "Name"]
   [:th "Time"]
   [:th "Valid?"]
   [:th "Results"]
   [:th "History"]
   [:th "Dir"]])

(defn path
  [t & args]
  (str "/file/" (str/replace (.getPath (apply store/path t args))
                             #"^store/" "")))

(defn test-row
  "Turns a test map into a table row."
  [t]
  (let [r (:results t)]
    [:tr
     [:td (:name t)]
     [:td (:start-time t)]
     [:td {:style (str "background: " (case (:valid? r)
                                        true            "#ADF6B0"
                                        false           "#F6AEAD"
                                        :indeterminate  "#F3F6AD"))}
      (:valid? r)]
     [:td [:a {:href (path t "results.edn")} "results.edn"]]
     [:td [:a {:href (path t "history.txt")} "history.txt"]]
     [:td [:a {:href (path t "")} "dir"]]]))

(defn home
  [req]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (h/html [:h1 "Jepsen"]
                 [:table
                  [:thead (test-header)]
                  [:tbody (->> (fast-tests)
                               (sort-by :start-time)
                               reverse
                               (map test-row))]])})

(defn file
  [req]
  (info "file" (:uri req))
  (assert (not (re-find #"\.\." (:uri req))))
  (let [path ((re-find #"^/file/(.+)$" (:uri req)) 1)]
    (response/file-response path
                            {:root             store/base-dir
                             :index-files?     false
                             :allow-symlinks?  false})))

(defn app [req]
;  (info :req (with-out-str (pprint req)))
  (let [req (assoc req :uri (java.net.URLDecoder/decode (:uri req) "UTF-8"))]
    (condp re-find (:uri req)
      #"^/$"     (home req)
      #"^/file/" (file req)
      {:status 404
       :headers {"Content-Type" "test/plain"}
       :body    "sorry, sorry--sorry!"})))

(defn serve!
  "Starts an http server with the given httpkit options."
  ([options]
   (let [s (server/run-server app options)]
     (info "Web server running.")
     s)))
