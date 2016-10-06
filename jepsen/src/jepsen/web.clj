(ns jepsen.web
  "Web server frontend for browsing test results."
  (:require [jepsen.store :as store]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [hiccup.core :as h]
            [ring.util.response :as response]
            [org.httpkit.server :as server])
  (:import (java.io File)
           (java.nio CharBuffer)
           (java.nio.file Path)))

; Path/File protocols
(extend-protocol io/Coercions
  Path
  (as-file [p] (.toFile p))
  (as-url [p] (.toURL (.toURI p))))

(defn url-encode-path-components
  "URL encodes *individual components* of a path, leaving / as / instead of
  encoded."
  [x]
  (str/replace (java.net.URLEncoder/encode x) #"%2F" "/"))

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
   [:th "History"]])

(defn relative-path
  "Relative path, as a Path."
  [base target]
  (let [base   (.toPath (io/file base))
        target (.toPath (io/file target))]
    (.relativize base target)))

(defn url
  "Takes a test and filename components; returns a URL for that file."
  [t & args]
  (url-encode-path-components
    (str "/files/" (str/replace (.getPath (apply store/path t args))
                                #"^store/" ""))))

(defn file-url
  "URL for a File"
  [f]
  (url-encode-path-components
    (str "/files/" (->> f io/file .toPath (relative-path store/base-dir)))))

(defn test-row
  "Turns a test map into a table row."
  [t]
  (let [r (:results t)]
    [:tr
     [:td [:a {:href (url t "")} (:name t)]]
     [:td [:a {:href (url t "")} (:start-time t)]]
     [:td {:style (str "background: " (case (:valid? r)
                                        true            "#ADF6B0"
                                        false           "#F6AEAD"
                                        :unknown        "#F3F6AD"
                                                        "#eaeaea"))}
      (:valid? r)]
     [:td [:a {:href (url t "results.edn")} "results.edn"]]
     [:td [:a {:href (url t "history.txt")} "history.txt"]]]))

(defn home
  "Home page"
  [req]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (h/html [:h1 "Jepsen"]
                 [:table {:cellspacing 3
                          :cellpadding 3}
                  [:thead (test-header)]
                  [:tbody (->> (fast-tests)
                               (sort-by :start-time)
                               reverse
                               (map test-row))]])})

(defn dir-cell
  "Renders a File (a directory) for a directory view."
  [^File f]
  [:a {:href (file-url f)
       :style "text-decoration: none;
              color: #000;"}
   [:div {:style "display: inline-block;
                 margin: 10px;
                 padding: 10px;
                 background: #F4E7B7;
                 overflow: hidden;
                 width: 280px;"}
    (.getName f)]])

(defn file-cell
  "Renders a File for a directory view."
  [^File f]
  [:div {:style "display: inline-block;
                margin: 10px;
                overflow: hidden;"}
   [:div {:style "height: 200px;
                  width: 300px;
                  overflow: hidden;"}
    [:a {:href (file-url f)
         :style "text-decoration: none;
                 color: #555;"}
     (cond
       (re-find #"\.(png|jpg|jpeg|gif)$" (.getName f))
       [:img {:src (file-url f)
              :title (.getName f)
              :style "width: auto;
                     height: 200px;"}]

       (re-find #"\.(txt|edn|json|yaml|log|stdout|stderr)$" (.getName f))
       [:pre
        (with-open [r (io/reader f)]
          (let [buf (CharBuffer/allocate 4096)]
            (.read r buf)
            (.flip buf)
            (.toString buf)))]

       true
       [:div {:style "background: #F4F4F4;
                     width: 100%;
                     height: 100%;"}])]]

   [:a {:href (file-url f)} (.getName f)]])

(defn dir
  "Serves a directory."
  [^File dir]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (h/html (->> dir
                      (.toPath)
                      (iterate #(.getParent %))
                      (take-while #(-> store/base-dir
                                       io/file
                                       .getCanonicalFile
                                       .toPath
                                       (not= (.toAbsolutePath %))))
                      (drop 1)
                      reverse
                      (map (fn [^Path component]
                             [:a {:href (file-url component)}
                                    (.getFileName component)]))
                      (cons [:a {:href "/"} "jepsen"])
                      (interpose " / "))
                 [:h1 (.getName dir)]
                 [:div
                  (->> dir
                       .listFiles
                       (filter (fn [^File f] (.isDirectory f)))
                       sort
                       (map dir-cell))]
                 [:div
                  (->> dir
                       .listFiles
                       (remove (fn [^File f] (.isDirectory f)))
                       (sort-by (fn [^File f]
                                  [(not= (.getName f) "results.edn")
                                   (not= (.getName f) "history.txt")
                                   (.getName f)]))
                       (map file-cell))])})

(defn files
  "Serve requests for /files/ urls"
  [req]
  (let [pathname ((re-find #"^/files/(.+)$" (:uri req)) 1)
        f    (File. store/base-dir pathname)]
    (assert (.startsWith (.toPath (.getCanonicalFile f))
                         (.toPath (.getCanonicalFile (io/file store/base-dir))))
            "File out of scope.")
    (cond
      (.isFile f)
      (response/file-response pathname
                              {:root             store/base-dir
                               :index-files?     false
                               :allow-symlinks?  false})

      (.isDirectory f)
      (dir f))))

(defn app [req]
;  (info :req (with-out-str (pprint req)))
  (let [req (assoc req :uri (java.net.URLDecoder/decode (:uri req) "UTF-8"))]
    (condp re-find (:uri req)
      #"^/$"     (home req)
      #"^/files/" (files req)
      {:status 404
       :headers {"Content-Type" "test/plain"}
       :body    "sorry, sorry--sorry!"})))

(defn serve!
  "Starts an http server with the given httpkit options."
  ([options]
   (let [s (server/run-server app options)]
     (info "Web server running.")
     s)))
