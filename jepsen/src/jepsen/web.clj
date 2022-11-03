(ns jepsen.web
  "Web server frontend for browsing test results."
  (:require [jepsen.store :as store]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-time [coerce :as time.coerce]
                      [core :as time]
                      [local :as time.local]
                      [format :as timef]]
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
  (as-file [^Path p] (.toFile p))
  (as-url [^Path p] (.toURL (.toUri p))))

(defn url-encode-path-components
  "URL encodes *individual components* of a path, leaving / as / instead of
  encoded."
  [^String x]
  (str/replace (java.net.URLEncoder/encode x "UTF-8") #"%2F" "/"))

(def test-cache
  "An in-memory cache of {:name, :start-time, :valid?} maps, indexed by an
  ordered map of [:start-time :name]. Earliest start times at the front."
  (atom (sorted-map-by (comp - compare))))

(def test-cache-key
  "Function which extracts the key for the test cache from a map."
  (juxt :start-time :name))

(def test-cache-mutable-window
  "How far back in the test cache do we refresh on every page load?"
  2)

(def page-limit
  "How many test rows per page?"
  128)

(def basic-date-time (timef/formatters :basic-date-time))

(defn parse-time
  "Parses a time from a string"
  [t]
  (-> (timef/parse-local basic-date-time t)
      time.coerce/to-date-time))

(defn fast-tests
  "Abbreviated set of tests: just name, start-time, results. Memoizes
  (partially) via test-cache."
  []
  (let [cached @test-cache
        ; Drop the most recent n keys from the cache--these we assume may have
        ; changed recently.
        cached (->> (keys cached)
                    (take test-cache-mutable-window)
                    (reduce dissoc cached))]
    (->> (for [[name runs] (store/tests)
               [time test] runs]
           (let [t {:name           name,
                    :start-time     (parse-time time)}]
             (or (get cached (test-cache-key t))
                 ; Fetch from disk if not cached. Note that we construct a new
                 ; map so we can release the filehandle associated with the lazy
                 ; test map.
                 (try
                   (let [results {:valid? (:valid? (store/load-results
                                                     name time)
                                                   :incomplete)}
                         t (-> t (assoc :results results))]
                     (swap! test-cache assoc (test-cache-key t) t)
                     t)
                   (catch java.io.EOFException e
                     (assoc t :results {:valid? :incomplete}))
                   (catch java.io.FileNotFoundException e
                     (assoc t :results {:valid? :incomplete}))
                   (catch java.lang.RuntimeException e
                     ; Um???
                     (warn e "Unable to parse" (str name "/" time))
                     (assoc t :results {:valid? :incomplete}))))))
         (sort-by :start-time)
         reverse
         vec)))

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
    (str "/files/" (-> ^File (apply store/path t args)
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
  (let [r    (:results t)
        time (->> t
                  :start-time
                  (timef/unparse (timef/formatters :date-hour-minute-second)))]
    [:tr
     [:td [:a {:href (url t "")} (:name t)]]
     [:td [:a {:href (url t "")} time]]
     [:td {:style (str "background: " (valid-color (:valid? r)))}
      (:valid? r)]
     [:td [:a {:href (url t "results.edn")}    "results.edn"]]
     [:td [:a {:href (url t "history.txt")}    "history.txt"]]
     [:td [:a {:href (url t "jepsen.log")}     "jepsen.log"]]
     [:td [:a {:href (str (url t) ".zip")} "zip"]]]))

(defn params
  "Parses a query params map from a request."
  [req]
  (when-let [q (:query-string req)]
    (->> (str/split q #"&")
         (keep (fn [pair]
                 (when (not= "" pair)
                   (let [[k v] (str/split pair #"=")]
                     [(keyword k) v]))))
         (into {}))))

(defn home
  "Home page"
  [req]
  (let [params (params req)
        after (if-let [a (:after params)]
                (parse-time a)
                ; In the year three thousaaaaaand
                (parse-time "30000101T000000.000Z"))
        tests (->> (fast-tests)
                   (drop-while (fn [t]
                                 (->> (:start-time t)
                                      (time/after? after)
                                      not))))
        more? (< page-limit (count tests))
        tests (take page-limit tests)]
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (h/html
             [:h1 "Jepsen"]
             [:table {:cellspacing 3
                      :cellpadding 3}
              [:thead (test-header)]
              [:tbody (map test-row tests)]]
             (when more?
               [:p [:a {:href (str "/?after="
                                   (->> (last tests)
                                        :start-time
                                        time.coerce/to-date-time
                                        (timef/unparse basic-date-time)))}
                    "Older tests..."]]))}))

(defn dir-cell
  "Renders a File (a directory) for a directory view."
  [^File f]
  (let [results-file  (io/file f "results.edn")
        valid?        (try (with-open [r (java.io.PushbackReader.
                                           (io/reader results-file))]
                             (:valid?
                               (clojure.edn/read
                                 {:default vector}
                                 r)))
                           (catch java.io.FileNotFoundException e
                             nil)
                           (catch RuntimeException e
                             (info e :caught)
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
    (sort-by #(Long/parseLong (.getName ^File %)) files)
    (sort files)))

(defn js-escape
  "Escape a Javascript string."
  [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "'" "\\x27")
      (str/replace "\"" "\\x22")))

(defn clj-escape
  "Escape a Clojure string."
  [s]
  (-> (str/escape s {\" "\\\""
                     \\ "\\\\"})))

(defn dir
  "Serves a directory."
  [^File dir]
  {:status 200
   :headers {"Content-Type" "text/html"}
   ; Breadcrumbs
   :body (h/html (->> dir
                      (.toPath)
                      (iterate #(.getParent ^Path %))
                      (take-while #(-> store/base-dir
                                       io/file
                                       .getCanonicalFile
                                       .toPath
                                       (not= (.toAbsolutePath ^Path %))))
                      (drop 1)
                      reverse
                      (map (fn [^Path component]
                             [:a {:style "margin: 0 0.3em"
                                  :href (file-url component)}
                              (.getFileName component)]))
                      (cons [:a {:style "margin-right: 0.3em"
                                 :href  "/"}
                             "jepsen"])
                      (interpose "/"))
                 ; Title
                 (let [path (js-escape
                              (str \" (clj-escape (.getCanonicalPath dir)) \"))]
                   ; You can click to copy the full local path
                   [:h1 {:onclick (str "navigator.clipboard.writeText('"
                                       path "')")}
                    (.getName dir)
                    ; Or download a zip file
                    [:a {:style "font-size: 60%;
                                margin-left: 0.3em;"
                         :href (file-url (str dir ".zip"))}
                     ".zip"]])
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

(def content-type
  "Map of extensions to known content-types"
  {"txt"  "text/plain"
   "log"  "text/plain"
   "edn"  "text/plain" ; Wrong, but we like seeing it in-browser
   "json" "text/plain" ; Ditto
   "html" "text/html"
   "svg"  "image/svg+xml"})

(defn files
  "Serve requests for /files/ urls"
  [req]
  (let [pathname ((re-find #"^/files/(.+)\z" (:uri req)) 1)
        ext      (when-let [m (re-find #"\.(\w+)\z" pathname)] (m 1))
        f        (File. store/base-dir ^String pathname)]
    (assert-file-in-scope! f)
    (cond
      (.isFile f)
      (let [res (response/file-response pathname
                                        {:root             store/base-dir
                                         :index-files?     false
                                         :allow-symlinks?  false})]
          (if-let [ct (content-type ext)]
            (-> res
                (response/content-type ct)
                (response/charset "utf-8"))
            res))

      (= ext "zip")
      (zip req f)

      (.isDirectory f)
      (dir f)

      true
      e404)))

(defn app [req]
;  (info :req (with-out-str (pprint req)))
  (let [req (assoc req :uri (java.net.URLDecoder/decode
                              ^String (:uri req) "UTF-8"))]
    (condp re-find (:uri req)
      #"^/$"     (home req)
      #"^/files/" (files req)
      e404)))

(defn serve!
  "Starts an http server with the given httpkit options."
  ([options]
   (let [s (server/run-server app options)]
     (info "I'll see YOU after the function")
     s)))
