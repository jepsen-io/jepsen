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
  (:import (java.io File
                    FileInputStream
                    PipedInputStream
                    PipedOutputStream
                    OutputStream)
           (java.nio CharBuffer)
           (java.nio.file Path
                          Files)
           (java.nio.file.attribute FileTime)
           (java.util.zip ZipEntry
                          ZipOutputStream)))

(def colors
  {:ok   "#6DB6FE"
   :info "#FFAA26"
   :fail "#FEB5DA"
   nil   "#eaeaea"})

(def valid-color
  (comp colors {true     :ok
                :unknown :info
                false    :fail}))

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
                             {:name       test-name
                              :start-time test-time
                              :results    {:valid? :incomplete}})
                           (catch java.lang.RuntimeException e
                             ; Um???
                             {:name       test-name
                              :start-time test-time
                              :results    {:valid? :incomplete}})))
                       runs)))))

(defn test-header
  []
  [:tr
   [:th "Name"]
   [:th "Time"]
   [:th "Valid?"]
   [:th "Results"]
   [:th "History"]
   [:th "Log"]
   [:th "Zip"]])

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
    (str "/files/" (-> (apply store/path t args)
                       .getPath
                       (str/replace #"\Astore/" "")
                       (str/replace #"/\Z" "")))))

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
     [:td {:style (str "background: " (valid-color (:valid? r)))}
      (:valid? r)]
     [:td [:a {:href (url t "results.edn")}    "results.edn"]]
     [:td [:a {:href (url t "history.txt")}    "history.txt"]]
     [:td [:a {:href (url t "jepsen.log")}     "jepsen.log"]]
     [:td [:a {:href (str (url t) ".zip")} "zip"]]]))

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
  (let [results-file  (io/file f "results.edn")
        valid?        (try (with-open [r (java.io.PushbackReader.
                                           (io/reader results-file))]
                             (:valid? (clojure.edn/read r)))
                           (catch java.io.FileNotFoundException e
                             nil)
                           (catch RuntimeException e
                             :unknown))]
  [:a {:href (file-url f)
       :style "text-decoration: none;
              color: #000;"}
   [:div {:style (str "background: " (valid-color valid?) ";\n"
                      "display: inline-block;
                      margin: 10px;
                      padding: 10px;
                      overflow: hidden;
                      width: 280px;")}
    (.getName f)]]))

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

(defn dir-sort
  "Sort a collection of Files. If everything's an integer, sort numerically,
  else alphanumerically."
  [files]
  (if (every? (fn [^File f] (re-find #"^\d+$" (.getName f))) files)
    (sort-by #(Long/parseLong (.getName %)) files)
    (sort files)))

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
                       dir-sort
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

(defn zip-path!
  "Writes a path to a zipoutputstream"
  [^ZipOutputStream zipper base ^File file]
  (when (.isFile file)
    (let [relpath (str (relative-path base file))
          entry   (doto (ZipEntry. relpath)
                    (.setCreationTime (FileTime/fromMillis
                                        (.lastModified file))))
          buf   (byte-array 16384)]
      (with-open [input (FileInputStream. file)]
        (.putNextEntry zipper entry)
        (loop []
          (let [read (.read input buf)]
            (if (< 0 read)
              (do (.write zipper buf 0 read)
                  (recur))
              (.closeEntry zipper)))))))
  zipper)

(defn zip
  "Serves a directory as a zip file. Strips .zip off the extension."
  [req ^File dir]
  (let [f (-> dir
              (.getCanonicalFile)
              str
              (str/replace #"\.zip\z" "")
              io/file)
        pipe-in (PipedInputStream. 16384)
        pipe-out (PipedOutputStream. pipe-in)]
    (future
      (try
        (with-open [zipper (ZipOutputStream. pipe-out)]
          (doseq [file (file-seq f)]
            (zip-path! zipper f file)))
        (catch Exception e
          (warn e "Error streaming zip for" dir))
        (finally
          (.close pipe-out))))
    {:status  200
     :headers {"Content-Type" "application/zip"}
     :body    pipe-in}))

(defn assert-file-in-scope!
  "Throws if the given file is outside our store directory."
  [^File f]
  (assert (.startsWith (.toPath (.getCanonicalFile f))
                       (.toPath (.getCanonicalFile (io/file store/base-dir))))
          "File out of scope."))

(def e404
  {:status 404
   :headers {"Content-Type" "text/plain"}
   :body "404 not found"})

(defn files
  "Serve requests for /files/ urls"
  [req]
  (let [pathname ((re-find #"^/files/(.+)$" (:uri req)) 1)
        f    (File. store/base-dir pathname)]
    (assert-file-in-scope! f)
    (cond
      (.isFile f)
      (response/file-response pathname
                              {:root             store/base-dir
                               :index-files?     false
                               :allow-symlinks?  false})

      (re-find #"\.zip\z" pathname)
      (zip req f)

      (.isDirectory f)
      (dir f)

      true
      e404)))

(defn app [req]
;  (info :req (with-out-str (pprint req)))
  (let [req (assoc req :uri (java.net.URLDecoder/decode (:uri req) "UTF-8"))]
    (condp re-find (:uri req)
      #"^/$"     (home req)
      #"^/files/" (files req)
      e404)))

(defn serve!
  "Starts an http server with the given httpkit options."
  ([options]
   (let [s (server/run-server app options)]
     (info "Web server running.")
     s)))
