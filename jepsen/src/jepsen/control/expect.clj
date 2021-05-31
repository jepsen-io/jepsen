(ns jepsen.control.expect
  "When we run commands in a sudo subshell, we need a way to intercept stdin,
  look for password prompts, and respond with a password. The Expect protocol
  formalizes how jepsen.control `exec` calls interact with stdin, stdout, and
  stderr.

    remote                               local
    process                              jepsen exec
              ┌────────┐   ┌────────┐
    stdin  <──│        │<──│        │<── stdin
              │        │   │        │
    stdout ──>│ Expect │──>│ Expect │──> stdout
              │        │   │        │
    stderr ──>│        │──>│        │──> stderr
              └────────┘   └────────┘

  When the process is started, Jepsen's `exec` wants to provide some stdin to
  the process, and the process, in turn, may want to provide stdout and stderr
  to expect. These driving forces need to 'meet in the middle', where each
  `expect` balances them: blocking, transforming, or passing through streams as
  appropriate.

  To link Expects to each other, each has two optional references to other
  expects in its chain: `local` and `remote`. `downstream` could mean either
  local or remote, depending on whether we're talking about stdin or
  stdout/sterr.

    ┌────────┐   ┌────────┐   ┌────────┐
    │        │   │        │   │        │
    │ remote │   │        │   │ local  │
    │ Expect │<──│ Expect │──>│ Expect │
    │        │   │        │   │        │
    │        │   │        │   │        │
    └────────┘   └────────┘   └────────┘

  Because these links are bidirectional, they must be (initially) mutable. Each
  Expect receives one or zero calls to set-local! and set-remote! to configure
  these links, before any input is provided. Callers use `(link!
  coll-of-expects-from-most-remote-to-most-local)` to establish these linkages
  before a command begins.

  Expects are passive reactors to inputs, not autonomous threads--we're trying
  to keep this lightweight and efficient since execs are frequent. To this end,
  an exec has three ports which can be written to: one local, and two remote.

    remote                  local
    process                 jepsen exec
              ┌────────┐
              │        │<── stdin
              │        │
    stdout ──>│ Expect │
              │        │
    stderr ──>│        │
              └────────┘

  The actual calls used to write these ports are

    (in!  expect chunk)
    (out! expect chunk)
    (err! expect chunk)

  The form of the chunks can vary from Expect to Expect. At the process end,
  these are arrays of bytes. At the local end, we almost always want to deal in
  UTF-8 strings. To that end, we use a translation Expect which encodes and
  decodes data between bytes and Utf-8 strings.

  These channels can be flushed with `(flush-in! expect)`, `(flush-out!
  expect)`, and `(flush-err! expect)`, and closed with `(close-in! expect)`,
  etc.

  Because writers are performed by different threads, each Expect must be
  thread-safe.

  At the far-remote edge of the chain, a special Expect routes stdin off to the
  remote process, and receives stdout/stderr from the remote process. At the
  far-local edge, a special Expect sends any prepared input for the process,
  and buffers stdout/stderr to return them to the caller.

  Most Expects will have no initial state; however, those pulling data from
  inputstreams will need a thread to do so. For this, we have the (start!
  expect) function, which is called after linking, and allows Expects to launch
any threads they might need."
  (:require [clojure.tools.logging :refer [info warn]]
            [byte-streams :as bs]
            [potemkin :refer [definterface+]])
  (:import (java.util Arrays)
           (java.util.concurrent.locks ReentrantLock)
           (java.io ByteArrayOutputStream
                    InputStream
                    OutputStream)
           (java.nio ByteBuffer
                     CharBuffer)
           (java.nio.charset Charset
                             CharsetDecoder
                             CoderResult
                             CodingErrorAction)))

(definterface+ Expect
  (set-local! [expect local-expect]
              "Connects this expect to a more local Expect.")
  (set-remote! [expect local-expect]
              "Connects this expect to a more local Expect.")

  (start! [expect]
          "Called after local and remote are set; launches worker threads, if
          any.")

  (in! [expect chunk] "Sends a piece of stdin towards the remote process")
  (out! [expect chunk] "Sends a piece of stdout towards the local exec")
  (err! [expect chunk] "Sends a piece of stderr towards the local exec")

  (flush-in! [expect]  "Flushes internal stdin buffers, and those downstream")
  (flush-out! [expect] "Flushes internal stdout buffers, and those downstream")
  (flush-err! [expect] "Flushes internal stderr buffers, and those downstream")

  (close-in! [expect] "Closes this Expect and downstreams for further stdin")
  (close-out! [expect] "Closes this Expect and downstreams for further stdout")
  (close-err! [expect] "Closes this Expect and downstreams for forther stderr"))

(definterface+ ILocalAccumulator
  (out-str [this] "Returns accumulated stdout as a string.")
  (err-str [this] "Returns accumulated stderr as a string."))

(deftype LocalAccumulator [^:volatile-mutable remote
                           ^:volatile-mutable local
                           ^StringBuilder out
                           ^StringBuilder err]
  ILocalAccumulator
  (out-str [_] (.toString out))
  (err-str [_] (.toString err))

  Expect
  (start!      [_])
  (set-local!  [_ local']  (set! local local'))
  (set-remote! [_ remote'] (set! remote remote'))

  (in!  [_ chunk]  (in! remote chunk))
  (out! [_ chunk]
    (info "Accumulate out" chunk)
    (.append out ^String chunk))
  (err! [_ chunk]
    (info "Accumulate err" chunk)
    (.append err ^String chunk))

  (flush-in! [_] (flush-in! remote))
  (flush-out! [_])
  (flush-err! [_])

  (close-in! [_] (close-in! remote))
  (close-out! [_])
  (close-err! [_]))

(defn local-accumulator
  "This Expect passes on stdin to the remote process immediately, but
  accumulates all stdout and stderr in large strings, which can be read via
  out-str and err-str."
  []
  (LocalAccumulator. nil nil (StringBuilder.) (StringBuilder.)))

(defn remote-streams-worker
  "A future which copies bytes from an InputStream representing process stdout
  and sends them to this worker's local's corresponding stream. Takes functions
  to send, flush, and close (e.g. out!, flush-out!, close-out!). When the
  underlying InputStream is exhausted, flushes and closes."
  [this local send-fn! flush-fn! close-fn! ^InputStream in]
  (future
    (try
      (loop []
        (info "Reading from in")
        (let [buf         (byte-array (.available in))
              read-bytes  (.read in buf)]
          (info "Got" read-bytes "bytes")
          (condp = read-bytes
            ; Nothing read
            0 (do (Thread/sleep 1000)
                  (recur))
            ; EOF
            -1 :eof

            ; Ah, good, we read some bytes. Truncate the byte array and send it
            ; downstream.
            (let [slice (Arrays/copyOf buf read-bytes)]
              (send-fn! local slice)
              (recur)))))
      (catch RuntimeException e
        (warn e "Processing input stream"))
      (finally
        ; We can close the inputstream, if it hasn't been already.
        (.close in)
        ; Great, we need to flush the entire chain
        (flush-fn! this)
        ; And now that we're closed, we can close the downstream
        ; chain.
        (close-fn! local)))))

(deftype RemoteStreams [^:volatile-mutable remote
                        ^:volatile-mutable local
                        ; Futures
                        ^:volatile-mutable out-worker
                        ^:volatile-mutable err-worker
                        ; Streams
                        ^OutputStream in
                        ^InputStream out
                        ^InputStream err]
  Expect
  (start! [this]
    ; Launch threads to transfer inputstreams to local expects
    (set! out-worker (remote-streams-worker this local out! flush-out! close-out! out))
    (set! out-worker (remote-streams-worker this local err! flush-err! close-err! err))
    this)

  (set-local!  [_ local']  (set! local local'))
  (set-remote! [_ remote'] (set! remote remote'))

  (in! [_ chunk]
    (.write in chunk))

  ; Not used
  (out! [_ chunk])
  (err! [_ chunk])

  (flush-in!  [_] (.flush in))
  (flush-out! [_] (flush-out! local))
  (flush-err! [_] (flush-err! local))

  ; To close the input stream, we simply close our wrapped stream.
  (close-in! [this]
    (flush-in! this)
    (.close in))

  ; To close the output stream, we first close the stream, which causes the
  ; worker to finish. We block on the worker's completion.
  (close-out! [this]
    (.close out)
    (and out-worker @out-worker))

  (close-err! [this]
    (.close err)
    (and err-worker @err-worker)))

(defn remote-streams
  "An Expect for the Input & OutputStreams attached to a remote process. Writes
  to this Expect are sent to the corresponding stream; inputs are copied by
  worker threads."
  [in out err]
  (RemoteStreams. nil nil
                  nil nil
                  in out err))

(def utf8-name "UTF-8")
(def decode-buffer-size
  "Size of the UTF-8 decoding character buffer"
  16384)

(defn decode!
  "A wrapper for CharsetDecoder/decode. Returns :underflow, :overflow, or
  throws if a decoding error occurs."
  [^CharsetDecoder decoder ^ByteBuffer in ^CharBuffer out eof?]
  (let [result (.decode decoder in out eof?)]
    (cond (.isUnderflow result) :underflow
          (.isOverflow result)  :overflow
          true                  (.throwException result))))


(deftype UTF8 [^:volatile-mutable remote
               ^:volatile-mutable local
               ^CharsetDecoder out-decoder
               ^CharsetDecoder err-decoder
               ^CharBuffer     out-buf
               ^CharBuffer     err-buf
               ]
  Expect
  (start! [_])
  (set-local!  [_ local']  (set! local local'))
  (set-remote! [_ remote'] (set! remote remote'))

  (in!  [_ chunk] (in! remote (.getBytes ^String chunk utf8-name)))

  (out! [_ chunk]
    ; Start by converting this chunk of bytes to a buffer, and decoding it
    (let [^ByteBuffer in-buf (bs/to-byte-buffer chunk)]
      (loop []
        ; Decode this chunk into the stdout charbuf
        (case (decode! out-decoder in-buf out-buf false)
          ; We've maximally decoded this input chunk. Send this string
          ; downstream and reset the output buffer for next time.
          :underflow (do (out! local (.toString out-buf))
                         (.clear out-buf))

          ; We filled up the output buffer. Send it downstream, reset the
          ; buffer, and retry decoding.
          :overflow (do (out! local (.toString out-buf))
                        (.clear out-buf)
                        (recur))))))

  (err! [_ chunk]
    ; Start by converting this chunk of bytes to a buffer, and decoding it
    (let [^ByteBuffer in-buf (bs/to-byte-buffer chunk)]
      (loop []
        ; Decode this chunk into the stderr charbuf
        (case (decode! err-decoder in-buf err-buf false)
          ; We've maximally decoded this input chunk. Send this string
          ; downstream and reset the errput buffer for next time.
          :underflow (do (err! local (.toString err-buf))
                         (.clear err-buf))

          ; We filled up the errput buffer. Send it downstream, reset the
          ; buffer, and retry decoding.
          :overflow (do (err! local (.toString err-buf))
                        (.clear err-buf)
                        (recur))))))

  (flush-in!  [_] (flush-in!  remote))
  (flush-out! [_]
    (out! local (.toString out-buf))
    (.clear out-buf)
    (flush-out! local))

  (flush-err! [_]
    (err! local (.toString err-buf))
    (.clear err-buf)
    (flush-err! local))

  (close-in!  [_] (close-in! remote))

  (close-out! [this]
    ; Final decode step! Decode an empty buffer and flush
    (decode! out-decoder (ByteBuffer/allocate 0) out-buf true)
    (let [result (.flush out-decoder)]
      (when (.isOverflow result)
        ; I'm REALLY surprised if this happens: our buffer is huge compared to
        ; what I assume the internal decoder state can hold.
        (throw (IllegalStateException.
                 "Failed to flush buffer: overflow? Somehow???")))
      (when-not (.isUnderflow result)
        (.throwException result))
      ; Right, we've flushed the decoder; now we need to send our final state
      ; along and close.
      (flush-out! this)
      (close-out! local)))

  (close-err! [this]
    ; Final decode step! Decode an empty buffer and flush
    (decode! err-decoder (ByteBuffer/allocate 0) err-buf true)
    (let [result (.flush err-decoder)]
      (when (.isOverflow result)
        ; I'm REALLY surprised if this happens: our buffer is huge compared to
        ; what I assume the internal decoder state can hold.
        (throw (IllegalStateException.
                 "Failed to flush buffer: overflow? Somehow???")))
      (when-not (.isUnderflow result)
        (.throwException result))
      ; Right, we've flushed the decoder; now we need to send our final state
      ; along and close.
      (flush-err! this)
      (close-err! local)))
  )

(defn utf8-decoder
  "Constructs a new Decoder for decoding a stream of UTF-8 chars."
  []
  (doto (.newDecoder (Charset/forName utf8-name))
    (.onMalformedInput CodingErrorAction/REPORT)
    (.onUnmappableCharacter CodingErrorAction/REPORT)))

(defn utf8
  "This Expect translates between chunks of bytes on the remote side, and
  chunks of UTF-8 Strings on the local side."
  []
  (UTF8. nil nil
         (utf8-decoder)
         (utf8-decoder)
         (CharBuffer/allocate decode-buffer-size)
         (CharBuffer/allocate decode-buffer-size)))

(defn link!
  "Takes a collection of Expects, from most remote to most local. Establishes
  the local/remote linkages between each adjacent pair. Returns expects."
  [expects]
  (->> expects
       (partition 2 1)
       (map (fn [[remote local]]
              (set-local! remote local)
              (set-remote! local remote)))
       dorun)
  expects)

(defn start!
  "Takes a collection of Expects, and starts each one. Returns expects."
  [expects]
  (doseq [e expects]
    (start! e))
  expects)

(defn remote-end
  "Returns the expect on the far remote end of a chain."
  [expects]
  (first expects))

(defn local-end
  "Returns the expect on the far local end of a chain."
  [expects]
  (assert (vector? expects))
  (peek expects))

(defn close!
  "Closes down an entire chain of expects. Returns expects."
  [expects]
  (close-in! (local-end expects))
  (close-out! (remote-end expects))
  (close-err! (remote-end expects))
  expects)
