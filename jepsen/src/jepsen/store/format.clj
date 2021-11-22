(ns jepsen.store.format
  "Jepsen tests are logically a map. To save this map to disk, we originally
  wrote it as a single Fressian file. This approach works reasonably well, but
  has a few problems:

  - We write test files multiple times: once at the end of a test, and once
  once the analysis is complete--in case the analysis fails. Rewriting the
  entire file is inefficient. It would be nice to incrementally append new
  state.

  - Histories are *enormous* relative to tests, but we force readers to
  deserialize them before being able to get to any other high-level keys in the
  test--for instance, the result map.

  - It might be nice, someday, to have histories bigger than fit into memory.

  - We have no way to incrementally write the history, which means if a test
  crashes during the run we lose everything.

  - Deserializing histories is a linear process, but it would be nice for
  analyses to be able to parallelize.

  - The web view needs a *little* metadata quickly: the name, the date, the
  valid field of the result map. Forcing it to deserialize the entire world to
  get this information is bad.

  - Likewise, loading tests at the REPL is cumbersome--if all one wants is the
  results, you should be able to skip the history. Working with the history
  should ideally be lazy.

  I held off on designing a custom serialization format for Jepsen for many
  years, but at this point the design constraints feel pretty well set, and I
  think the time is right to design a custom format.


  ## File Format Structure

  Jepsen files begin with the magic UTF8 string JEPSEN, followed by a 32-byte
  big-endian unsigned integer version field, which we use to read old formats
  when necessary. This is version 0. There follows a series of *blocks*:

         6            32
    | \"JEPSEN\" | version | block 1 | block 2 | ...

  All integers are signed and big-endian, unless otherwise noted. This is the
  JVM, after all.

  Blocks may be sparse--their lengths may be shorter than the distance to the
  start of the next block. This is helpful when one needs to rewrite blocks
  later: you can leave padding for their sizes to change.

  The first block is an index block. The top-level value of the file (e.g. the
  test map) is stored in block 1.


  ## Block Structure

  All blocks begin with an 8-byte length prefix which indicates the length of
  the block in bytes, including the length prefix itself. Then follows a CRC32
  checksum, which applies to the entire block including the length header,
  except for the checksum field itself, which is zeroed out for computing the
  checksum. Third, we have a 16-bit block type field, which identifies how to
  interpret the block. Finally, we have the block's data, which is
  type-dependent.

        64        32       16
    | length | checksum | type | ... data ...


  ## Index Blocks (Type 1)

  An index block maps logical block numbers to file offsets. It consists of a
  series of pairs: each pair a 32-bit logical block ID and an offset into the
  file.

       32       64       32      64
    | id 1 | offset 1 | id 2 | offset 2 | ...

  There is no block with ID 0: 0 is used as a nil sentinel.

  Index blocks are deliberately spaced out from the following block so that
  they can be rewritten when more blocks are added.


  ## Fressian Blocks (Type 2)

  A *Fressian block* encodes data (often a key-value map) using the Fressian
  serialization format. This is already the workhorse for Jepsen serialization,
  but we introduce a twist: large values, like the history and results, can be
  stored in other blocks. That way you don't have to deserialize the entire
  thing in order to read the top-level structure.

  We create a special datatype, BlockRef, which we encode as a 'block-ref' tag
  in Fressian. This ref simply contains the ID of the block which encodes that
  tag's value.

    | fressian data ... |


  ## PartialMap (Type 3)

  Results are a bit weird. We want to efficiently fetch the :valid? field from
  them, but the rest of the result map could be *enormous*. To speed this up,
  we want to be able to write *part* of a map (for instance, just the results
  :valid? field), and store the rest in a different block.

  A PartialMap is essentially a cons cell: it comprises a Fressian-encoded map
  and a pointer to the ID of a *rest* block (also a PartialMap) which encodes
  the remainder of the map. This makes access to those parts of the map encoded
  in the head cell fast.

         32
    | rest-ptr | fressian data ...

  When rest-ptr is 0, that indicates there is no more data remaining.


  ## That's It

  There's a lot of obvious stuff I've left out here--metadata, top-level
  integrity checks, garbage collection, large vector indices for
  parallelization, etc etc... but I think we can actually skip almost all of it
  and get a ton of benefit for the limited use case Jepsen needs.

  1. Write the header.

  2. Write an index block (block 1) with enough space for, say, a dozen slots.

  3. Write the test map as a Fressian block to block 2 (the test map), with
  :history and :results pointing to blocks 3 and 4. Update the index block.

  4. Write the history as a Fressian block to block 3. Update the index block.

  5. Write the results as a PartialMap to blocks 4 and 5: 4 containing the
  :valid? field, and 5 containing the rest of the results. Update the index
  block.

  6. The test may contain state which changed by the end of the test, and we
  might want to save that state. Write the final test map as a new Fressian
  block at the end of the test, again referencing blocks 3 and 4 for the
  history and results. Flip the block index for block 1 to point to this block.
  Zero out the original block 1, if we like. Probably not worth preserving
  history.

  To read this file, we:

  1. Check the magic and version

  2. Read the index block into memory.

  3. Look up block 2 and decode it as Fressian. Wrap it in a lazy map
  structure.

  When it comes time to reference the results or history in that lazy map, we
  look up the right block in the block index, seek to that offset, and decode
  whatever's there.

  Decoding a block is straightforward. We grab the length header, run a CRC
  over that region of the file, check the block type, then decode the remaining
  data based on the block structure."
  (:require [byte-streams :as bs]
            [clojure.data.fressian :as fress]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java.io :as io]
            [dom-top.core :refer [assert+]]
            [jepsen [util :as util :refer [map-vals]]]
            [jepsen.store.fressian :as jsf]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io BufferedOutputStream
                    File
                    InputStream
                    OutputStream
                    PipedInputStream
                    PipedOutputStream)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel
                              FileChannel$MapMode)
           (java.nio.file StandardOpenOption)
           (java.util.zip CRC32)
           (jepsen.store.format FileOffsetOutputStream)))

(def magic
  "The magic string at the start of Jepsen files."
  "JEPSEN")

(def magic-size
  "Bytes it takes to store the magic string."
  (count magic))

(def magic-offset
  "Where the magic is written"
  0)

(def version
  "The current file version."
  0)

(def version-size
  "Bytes it takes to store a version."
  4)

(def version-offset
  "Where in the file the version begins"
  (+ magic-offset magic-size))

(def first-block-offset
  "Where in the file the first block begins."
  (+ version-offset version-size))

(def block-len-size
  "How long is the length prefix for a block?"
  8)

(def block-len-offset
  "Where do we write a block length in a block header?"
  0)

(def block-checksum-size
  "How long is the checksum for a block?"
  4)

(def block-checksum-offset
  "Where do we write a checksum in the block header?"
  (+ block-len-offset block-len-size))

(def block-type-size
  "How long is the type for a block?"
  16)

(def block-type-offset
  "Where do we store the block type in a block header?"
  (+ block-checksum-offset block-checksum-size))

(def short->block-type
  "A map of integers to block types."
  {(short 1) :block-index
   (short 2) :fressian
   (short 3) :partial-map})

(def block-type->short
  "A map of block types to integer codes."
  (->> short->block-type (map (juxt val key)) (into {})))

(def block-header-size
  "How long is a block header?"
  (+ block-len-size block-checksum-size block-type-size))

(def block-id-size
  "How many bytes per block ID?"
  4)

(def block-offset-size
  "How many bytes per block offset address?"
  8)

(def block-index-count
  "How many blocks do we store, maximum, per file?"
  16)

(def block-index-size
  "How many bytes do we allocate to the block index structure, not including
  the header?"
  (* block-index-count (+ block-id-size block-offset-size)))

(defrecord BlockRef [block-id])

(defrecord Handle
  [^FileChannel file  ; The filechannel we use for reads and writes
   block-index        ; An atom to a block index: a map of block IDs to offsets
   ]

  java.lang.AutoCloseable
  (close [this]
    (reset! block-index :closed)
    (.close file)))

(defn open
  "Constructs a new handle for a Jepsen file of the given path (anything which
  works with io/file)."
  [path]
  (let [path  (-> path io/file .toPath)
        f     (FileChannel/open path
                                (into-array StandardOpenOption
                                            [StandardOpenOption/CREATE
                                             StandardOpenOption/READ
                                             StandardOpenOption/WRITE]))
        block-index (atom {})]
    (Handle. f block-index)))

(defn close
  "Closes a Handle"
  [handle]
  (.close (:file handle))
  nil)

(defn flush!
  "Flushes writes to a Handle to disk."
  [handle]
  (.force (:file handle) false)
  handle)

(defn write-header!
  "Takes a Handle and writes the initial magic bytes and version number."
  [handle]
  (let [buf  (ByteBuffer/allocate (+ magic-size version-size))
        file ^FileChannel (:file handle)]
    (.position file 0)
    (bs/transfer magic file {:close? false})
    (.putInt buf version)
    (.flip buf)
    (bs/transfer buf file {:close? false})))

(defn check-magic
  "Takes a Handle and reads the magic bytes, ensuring they match."
  [handle]
  (let [file (:file handle)
        buf  (ByteBuffer/allocate magic-size)]
    (let [read-bytes (.read file buf magic-offset)
          _          (.flip buf)
          fmagic     (bs/convert buf String)]
      (when (or (not= magic-size read-bytes)
                (not= magic fmagic))
        (throw+ {:type      ::magic-mismatch
                 :expected  magic
                 :actual    (if (= -1 read-bytes)
                              :eof
                              fmagic)}))))
  handle)

(defn check-version
  "Takes a Handle and reads the version, ensuring it matches."
  [handle]
  (let [file ^FileChannel (:file handle)
        buf  (ByteBuffer/allocate version-size)]
    (let [read-bytes (.read file buf version-offset)
          fversion   (.getInt buf 0)]
      (when-not (= version-size read-bytes)
                (= version fversion)
        (throw+ {:type      ::version-mismatch
                 :expected  version
                 :actual    (if (= -1 read-bytes)
                              :eof
                              fversion)}))))
  handle)

; Working with block headers

(defn ^ByteBuffer block-header
  "Returns a blank ByteBuffer for a block header. All fields zero."
  []
  (ByteBuffer/allocate block-header-size))

(defn block-header-type
  "Returns the type of a block header, as a keyword."
  [^ByteBuffer header]
  (short->block-type (.getShort header block-type-offset)))

(defn set-block-header-type!
  "Sets the type (a keyword) in a block header. Returns the header."
  [^ByteBuffer buf block-type]
  (let [type-short (assert+ (block-type->short block-type)
                            {:type        ::no-such-block-type
                             :block-type  block-type})]
    (.putShort buf block-type-offset type-short)))

(defn block-header-length
  "Fetches the length of a block header."
  [^ByteBuffer header]
  (.getLong header block-len-offset))

(defn set-block-header-length!
  "Sets the length in a block header. Returns the block header."
  [^ByteBuffer buf length]
  (.putLong buf block-len-offset length))

(defn block-header-checksum
  "Fetches the checksum of a block header."
  [^ByteBuffer header]
  (.getInt header block-checksum-offset))

(defn set-block-header-checksum!
  "Sets the checksum in a block header. Returns the block header."
  [^ByteBuffer buf checksum]
  (.putInt buf block-checksum-offset checksum))

(defn ^Integer block-checksum
  "Compute the checksum of a block, given two bytebuffers: one for the header,
  and one for the data."
  [^ByteBuffer header ^ByteBuffer data]
  (let [c       (CRC32.)
        ; Create a copy of the header with a zeroed-out checksum.
        header' (block-header)]
    (.put header' header)
    (set-block-header-checksum! header' 0)
    ; Compute checksum
    ;(info :checksumming)
    (.rewind header')
    ;(bs/print-bytes header')
    ;(println "---")
    (.update c header')
    (.rewind data)
    ;(bs/print-bytes data)
    (.update c data)
    ;(info :checksum (unchecked-int (.getValue c)))
    (unchecked-int (.getValue c))))

(defn check-block-checksum
  "Verifies the checksum of a block, given two ByteBuffers: one for the header,
  and one for the data."
  [^ByteBuffer header ^ByteBuffer data]
  (let [expected (block-header-checksum header)
        actual   (block-checksum header data)]
    (assert+ (= expected actual)
             {:type ::checksum-mismatch
              :expected expected
              :actual   actual})))

(defn ^ByteBuffer read-block-header
  "Fetches the ByteBuffer for a block header at the given offset."
  [handle ^long offset]
  (let [file ^FileChannel (:file handle)
        buf               (block-header)
        read-bytes        (.read file buf offset)]
    (assert+ (= read-bytes block-header-size)
             {:type   ::block-header-truncated
              :offset offset
              :length (max 0 read-bytes)})
    ;(info :read-block-header :offset offset
    ;      :type     (block-header-type buf)
    ;      :length   (block-header-length buf)
    ;      :checksum (block-header-checksum buf))
    (.rewind buf)
    buf))

(defn ^ByteBuffer read-block-data
  "Fetches the ByteBuffer for a block's data, given a block header stored at
  the given offset."
  [handle offset header]
  (let [file        ^FileChannel (:file handle)
        data-length (- (block-header-length header) block-header-size)
        buf         (ByteBuffer/allocateDirect data-length)
        read-bytes  (.read file buf (+ offset block-header-size))]
    ;(info :read-block-data :offset offset
    ;      :block-header-size block-header-size
    ;      :data-length       data-length
    ;      :data              "\n"
    ;      (with-out-str (bs/print-bytes (.rewind buf))))
    (assert+ (= read-bytes data-length)
             {:type     ::block-data-truncated
              :offset   offset
              :expected data-length
              :actual   read-bytes})
    (.rewind buf)
    buf))

(defn write-block-header!
  "Writes a block header to the given offset in the file backed by the given
  handle. Returns handle."
  [handle ^long offset ^ByteBuffer block-header]
  (.rewind block-header)
  (let [written (.write ^FileChannel (:file handle) block-header offset)]
    (assert+ (= written block-header-size)
             {:type     ::block-header-write-failed
              :written  written
              :expected block-header-size}))
  ;(info :wrote-block-header :offset offset
  ;      :type     (block-header-type block-header)
  ;      :length   (block-header-length block-header)
  ;      :checksum (block-header-checksum block-header))
  handle)

(defn write-block-data!
  "Writes block data to the given block offset (e.g. the address of the header,
  not the data itself) in the file, backed by the given handle. Returns
  handle."
  [handle ^long offset ^ByteBuffer data]
  (.rewind data)
  (let [written (.write ^FileChannel (:file handle)
                        data
                        (+ offset block-header-size))]
    ; (info :wrote-block-data written :bytes)
    (assert+ (= written (.limit data))
             {:type     ::block-data-write-failed
              :written  written
              :expected (.limit data)}))
  handle)

(defn ^ByteBuffer block-header-for-data
  "Takes a block type and a ByteBuffer of data, and constructs a block header
  whose type is the given type, and which has the appropriate length and
  checksum for the given data."
  [block-type ^ByteBuffer data]
  (let [header (block-header)]
    (-> header
        (set-block-header-type! block-type)
        (set-block-header-length! (+ block-header-size
                                     (.limit data)))
        (set-block-header-checksum! (block-checksum header data)))))

(defn write-block!
  "Writes a block to a handle at the given offset, given a block type as a
  keyword and a ByteBuffer for the block's data. Returns handle."
  [handle ^long offset block-type data]
  (-> handle
      (write-block-header! offset (block-header-for-data block-type data))
      (write-block-data!   offset data)))

(defn read-block-by-offset
  "Takes a Handle and the offset of a block. Reads the block header and data,
  validates the checksum, and returns a map of:

    {:header header, as bytebuffer
     :data   data, as bytebuffer}"
  [handle offset]
  (let [file    ^FileChannel (:file handle)
        header  (read-block-header handle offset)
        data    (read-block-data handle offset header)]
    (check-block-checksum header data)
    (.rewind data)
    {:header header
     :data   data}))

;; Block indices

(defn write-block-index!
  "Writes the block index for a Handle, based on whatever that Handle's current
  block index is. If handle doesn't have a block index entry for the block
  index itself, adds one. Returns handle."
  [handle]
  (let [file    ^FileChannel (:file handle)
        data    (ByteBuffer/allocate block-index-size)
        index   (swap! (:block-index handle)
                       (fn ensure-block-index-block [index]
                         (if (contains? index (int 1))
                           index
                           (assoc index (int 1) first-block-offset))))]
    ; Make sure the block index fits
    (assert+ (<= (count index) block-index-count)
             {:type  ::block-index-too-large
              :limit block-index-count
              :count (count index)})
    ; Write each entry to the buffer
    (doseq [[id offset] index]
      (when-not (= :reserved offset)
        (.putInt data id)
        (.putLong data offset)))
    (.flip data)

    ; Write the header and data to the file.
    (write-block! handle first-block-offset :block-index data))
  handle)

(defn read-block-index
  "Reads the block index structure from a Handle. Returns a map of ids to
  offsets, or throws if the Handle doesn't contain a block index yet."
  [handle]
  (let [{:keys [^ByteBuffer header
                ^ByteBuffer data]} (read-block-by-offset handle
                                                         first-block-offset)
        ; Make sure this is a block index
        _ (assert+ (= :block-index (block-header-type header))
                   {:type     ::block-type-mismatch
                    :offset   first-block-offset
                    :expected :block-index
                    :actual   (block-header-type header)})]
    ; Great, interpret the data
    (loop [index (transient {})]
      (if (.hasRemaining data)
        (let [id      (.getInt data)
              offset  (.getLong data)]
          (recur (assoc! index id offset)))
        (persistent! index)))))

(defn refresh-block-index!
  "Takes a handle, reloads its block index from disk, and returns handle."
  [handle]
  (reset! (:block-index handle) (read-block-index handle))
  handle)

(defn read-block-by-id
  "Takes a handle and a logical block id. Assumes that the handle has a
  loaded block id. Looks up the offset for the given block, reads it using
  read-block-by-offset* (which includes verifying the checksum), and returns a
  map of:

    {:header header, as bytebuffer
     :data   data, as bytebuffer}"
  [handle id]
  (assert (instance? Integer id) "Block ids are integers, not longs!")
  (if-let [offset (get @(:block-index handle) id)]
    (read-block-by-offset handle offset)
    (throw+ {:type             ::block-not-found
             :id               id
             :known-block-ids (sort (keys @(:block-index handle)))})))

(defn new-block-id!
  "Takes a handle and returns a fresh block ID for that handle, mutating the
  handle so that this ID will not be allocated again."
  [handle]
  (let [index (:block-index handle)
        i     @index
        id    (int (if (empty? i)
                     2 ; Leave 1 free for the block index block itself.
                     (inc (reduce max (keys i)))))]
    (swap! index assoc id :reserved)
    id))

(defn next-block-offset
  "Takes a handle and returns the offset of the next block. Right now this is
  just the end of the file."
  [handle]
  (max (.size ^FileChannel (:file handle))
       ; When we first ask for a block, leave room for the block index
       ; beforehand.
       (+ first-block-offset block-header-size block-index-size)))

(defn assoc-block!
  "Takes a handle, a block ID, and its corresponding offset. Updates the
  handle's block index (in-memory) to add this mapping. Returns handle."
  [handle id offset]
  (swap! (:block-index handle) assoc id offset)
  handle)

;; Fressian blocks

(defn write-buffer-to-file!
  "Takes a FileChannel, an offset, and a ByteBuffer. Writes the ByteBuffer to
  the FileChannel at the given offset completely. Returns number of bytes
  written."
  [^FileChannel file offset ^ByteBuffer buffer]
  (let [size    (.remaining ^ByteBuffer buffer)
        written (.write file buffer offset)]
    ; Gonna punt on this for now because the position semantics are
    ; tricky and I'm kinda hoping we never hit it
    (assert (= size written)
            (str "Expected to write " size " bytes, but only wrote "
                 written
                 " bytes; go back and figure out what to do here."))
    written))

(defn write-fressian-block!
  "Takes a handle, a byte offset, and some Clojure data. Writes that data to a
  Fressian block at the given offset. Returns handle."
  [handle offset data]
  ; First, write the data to the file directly; then we'll go back and write
  ; the header.
  (let [data-size (with-open [foos (FileOffsetOutputStream.
                                     (:file handle)
                                     (+ offset block-header-size))
                              bos  (BufferedOutputStream. foos 16384)
                              w    (jsf/writer bos)]
                    (fress/write-object w data)
                    (.flush bos)
                    (.bytesWritten foos))
        ; Construct a ByteBuffer over the region we just wrote
        data (.map ^FileChannel (:file handle)
                   FileChannel$MapMode/READ_ONLY
                   (+ offset block-header-size)
                   data-size)
        ; And build our header
        header (block-header-for-data :fressian data)]
    ; Now write the header; data's already in the file.
    (write-block-header! handle offset header)
  handle))

(defn read-fressian-block
  "Takes a handle and an id. Reads the Fressian block at the given offset and
  returns its data."
  [handle id]
  (let [{:keys [^ByteBuffer header, ^ByteBuffer data]}
        (read-block-by-id handle id)]
    (assert+ (= :fressian (block-header-type header))
             {:type     ::block-type-mismatch
              :id       id
              :expected :fressian
              :actual   (block-header-type header)})
    (with-open [is (bs/to-input-stream data)
                r  (jsf/reader is)]
      (-> (fress/read-object r)
          (jsf/postprocess-fressian)))))
