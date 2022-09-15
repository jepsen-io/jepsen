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
  when necessary. This is version 0. Then there is a 64-bit offset into the
  file where the *block index*--the metadata structure--lives. There follows a
  series of *blocks*:

         6            32             64
    | \"JEPSEN\" | version | block-index-offset | block 1 | block 2 | ...

  In general, files are written by appending blocks sequentially to the end of
  the file---this allows Jepsen to write files in (mostly) a single pass,
  without moving large chunks of bytes around. When one is ready to save the
  file, one writes a new index block to the end of the file which provides the
  offsets of all the (active) blocks in the file, and finally updates the
  block-index-offset at the start of the file to point to that most-recent
  index block.

  All integers are signed and big-endian, unless otherwise noted. This is the
  JVM, after all.

  Blocks may be sparse--their lengths may be shorter than the distance to the
  start of the next block. This is helpful if one needs to rewrite blocks
  later: you can leave padding for their sizes to change.

  The top-level value of the file (e.g. the test map) is given in the block
  index.


  ## Block Structure

  All blocks begin with an 8-byte length prefix which indicates the length of
  the block in bytes, including the length prefix itself. Then follows a CRC32
  checksum. Third, we have a 16-bit block type field, which
  identifies how to interpret the block. Finally, we have the block's data,
  which is type-dependent.

        64        32       16
    | length | checksum | type | ... data ...

  Checksums are computed by taking the CRC32 of the data region, THEN the block
  header: the length, the checksum (all zeroes, for purposes of computing the
  checksum itself), and the type. We compute checksums this way so that writers
  can write large blocks of data with an unknown size in a single pass.


  ## Index Blocks (Type 1)

  An index block lays out the overall arrangement of the file: it stores a map
  of logical block numbers to file offsets, and also stores a root id, which
  identifies the block containing the top-level test map. The root id comes
  first, and is followed by the block map: a series of pairs, each a 32-bit
  logical block ID and an offset into the file.

       32      32       64       32      64
    root id | id 1 | offset 1 | id 2 | offset 2 | ...

  There is no block with ID 0: 0 is used as a nil sentinel when one wishes to
  indicate the absence of a block.


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

  2. Write the initial test map as a PartialMap block to block 1, with no
  history or results. Write an index block pointing to 1 as the root.

  3. Write the history as a Fressian block to block 2.

  4. Write a new PartialMap at block 3 with {:history (block-ref 2)}, and
  rest-ptr 1. Write a new index block with block 3 as the root. This ensures
  that if we crash after completing the history, but before starting the
  analysis, we'll still be able to recover the history itself and retry the
  analysis.

  5. Write the results as a PartialMap to blocks 4 and 5: 4 containing the
  :valid? field, and 5 containing the rest of the results.

  6. The test may contain state which changed by the end of the test, and we
  might want to save that state. Write the entire test map again as block 6: a
  Fressian block now referencing blocks 3 and 4 for the history and results.
  Write a new index block with block 6 as the root.

  To read this file, we:

  1. Check the magic and version.

  2. Read the index block offset.

  3. Read the index block into memory.

  3. Look up the root block ID, use the index to work out its offset, read that
  block, and decode it into a lazy map structure.

  When it comes time to reference the results or history in that lazy map, we
  look up the right block in the block index, seek to that offset, and decode
  whatever's there.

  Decoding a block is straightforward. We grab the length header, run a CRC
  over that region of the file, check the block type, then decode the remaining
  data based on the block structure."
  (:require [byte-streams :as bs]
            [clojure [set :as set]
                     [walk :as walk]]
            [clojure.data.fressian :as fress]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java.io :as io]
            [dom-top.core :refer [assert+]]
            [jepsen [util :as util :refer [map-vals]]
                    [fs-cache :refer [write-atomic!]]]
            [jepsen.store.fressian :as jsf]
            [potemkin :refer [def-map-type
                              definterface+]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io BufferedOutputStream
                    Closeable
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
           (jepsen.store.format FileOffsetOutputStream)
           (org.fressian.handlers WriteHandler
                                  ReadHandler)))

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

(def block-id-size
  "How many bytes per block ID?"
  4)

(def block-offset-size
  "How many bytes per block offset address?"
  8)

(def block-index-offset-offset
  "Where in the file do we write the offset of the index block?"
  (+ version-offset version-size))

(def first-block-offset
  "Where in the file the first block begins."
  (+ block-index-offset-offset block-offset-size))

;; Block Headers

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

;; Perf tuning

(def large-region-size
  "How big does a file region have to be before we just mmap it instead of
  doing file reads?"
  (* 1024 1024)) ; 1M

(def fressian-buffer-size
  "How many bytes should we buffer before writing Fressian data to disk?"
  16384) ; 16K

(defrecord BlockRef [id])

(defn block-ref
  "Constructs a new BlockRef object pointing to the given block ID."
  [id]
  (BlockRef. id))

(defrecord Handle
  [^FileChannel file  ; The filechannel we use for reads and writes
   block-index        ; An atom to a block index: a map of block IDs to offsets
   written?           ; An atom: have we written to this file yet?
   read?              ; An atom: have we read this file yet?
   ]

  Closeable
  (close [this]
    (reset! block-index :closed)
    (.close file)))

(defn ^Handle open
  "Constructs a new handle for a Jepsen file of the given path (anything which
  works with io/file)."
  [path]
  (let [path  (-> path io/file .toPath)
        f     (FileChannel/open path
                                (into-array StandardOpenOption
                                            [StandardOpenOption/CREATE
                                             StandardOpenOption/READ
                                             StandardOpenOption/WRITE]))
        block-index (atom {:root   nil
                           :blocks {}})]
    (Handle. f block-index (atom false) (atom false))))

(defn close!
  "Closes a Handle"
  [handle]
  (.close ^FileChannel (:file handle))
  nil)

(defn flush!
  "Flushes writes to a Handle to disk."
  [handle]
  (.force ^FileChannel (:file handle) false)
  handle)

; General IO routines

(defn write-file!
  "Takes a FileChannel, an offset, and a ByteBuffer. Writes the ByteBuffer to
  the FileChannel at the given offset completely. Returns number of bytes
  written."
  [^FileChannel file offset ^ByteBuffer buffer]
  (let [size    (.remaining ^ByteBuffer buffer)
        written (.write file buffer offset)]
    ; Gonna punt on this for now because the position semantics are
    ; tricky and I'm kinda hoping we never hit it
    (assert+ (= size written)
             {:type     ::incomplete-write
              :offset   offset
              :expected size
              :actual   written})
    written))

(defn ^ByteBuffer read-file
  "Returns a ByteBuffer corresponding to a given file region. Uses mmap for
  large regions, or regular read calls for small ones."
  [^FileChannel file, ^long offset, ^long size]
  (if (<= size large-region-size)
    ; Small region: read directly
    (let [buf        (ByteBuffer/allocate size)
          bytes-read (.read file buf offset)]
      (assert+ (= size bytes-read)
               {:type     ::incomplete-read
                :offset   offset
                :expected size
                :actual   bytes-read})
      (.rewind buf))
    ; Big region: mmap
    (.map file FileChannel$MapMode/READ_ONLY offset size)))

; General file headers

(defn write-header!
  "Takes a Handle and writes the initial magic bytes and version number. Returns handle."
  [handle]
  (let [buf  (ByteBuffer/allocate (+ magic-size version-size))
        file ^FileChannel (:file handle)]
    (.position file 0)
    (bs/transfer magic file {:close? false})
    (.putInt buf version)
    (.flip buf)
    (write-file! file version-offset buf))
  handle)

(defn check-magic
  "Takes a Handle and reads the magic bytes, ensuring they match."
  [handle]
  (let [file ^FileChannel (:file handle)
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

(defn prep-write!
  "Called when we write anything to a handle. Ensures that we've written the
  header before doing anything else. Returns handle."
  [handle]
  (when (compare-and-set! (:written? handle) false true)
    (write-header! handle))
  handle)

(declare load-block-index!)

(defn prep-read!
  "Called when we read anything from a handle. Ensures that we've checked the
  magic, version, and loaded the block index."
  [handle]
  (when (compare-and-set! (:read? handle) false true)
    (-> handle check-magic check-version load-block-index!))
  handle)

; Fetching and updating the block index offset root pointer

(defn write-block-index-offset!
  "Takes a handle and the offset of a block index block to use as the new root.
  Updates the file's block pointer. Returns handle."
  [handle root]
  (let [buf (ByteBuffer/allocate block-offset-size)]
    (.putLong buf 0 root)
    (write-file! (:file handle) block-index-offset-offset buf))
  handle)

(defn read-block-index-offset
  "Takes a handle and returns the current root block index offset from its
  file. Throws :type ::no-block-index if the block index is 0 or the file is
  too short."
  [handle]
  (try+
    (let [buf ^ByteBuffer (read-file (:file handle)
                                     block-index-offset-offset
                                     block-offset-size)
          offset (.getLong buf 0)]
      (when (zero? offset)
        (throw+ {:type ::no-block-index}))
      offset)
    (catch [:type ::incomplete-read] e
      (throw+ {:type ::no-block-index}))))

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

(defn block-checksum-given-data-checksum
  "Computes the checksum of a block, given a ByteBuffer header, and an
  already-computed CRC32 checksum of the data. Useful for streaming writers
  which compute their own checksums while writing. Mutates data-crc in place; I
  can't figure out how to safely copy it."
  [^ByteBuffer header, ^CRC32 data-crc]
  (let [header' (-> (block-header)
                    (set-block-header-type!   (block-header-type header))
                    (set-block-header-length! (block-header-length header)))]
    (.update data-crc ^ByteBuffer header')
    (unchecked-int (.getValue data-crc))))

(defn ^Integer block-checksum
  "Compute the checksum of a block, given two bytebuffers: one for the header,
  and one for the data."
  [header, ^ByteBuffer data]
  (let [c (CRC32.)]
    (.rewind data)
    ;(bs/print-bytes data)
    (.update c data)
    (block-checksum-given-data-checksum header c)))

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
  (write-file! (:file handle) (+ offset block-header-size) data)
  handle)

(defn ^ByteBuffer block-header-for-length-and-checksum!
  "An optimized way to construct a block header, a block type, the length of a
  data region (not including headers) and the CRC checksum of that data.
  Mutates the checksum in place."
  [block-type data-length data-checksum]
  (let [header (block-header)]
    (-> header
        (set-block-header-type!     block-type)
        (set-block-header-length!   (+ block-header-size data-length))
        (set-block-header-checksum! (block-checksum-given-data-checksum
                                      header data-checksum)))))

(defn ^ByteBuffer block-header-for-data
  "Takes a block type and a ByteBuffer of data, and constructs a block header
  whose type is the given type, and which has the appropriate length and
  checksum for the given data."
  [block-type ^ByteBuffer data]
  (let [header (block-header)]
    (-> header
        (set-block-header-type!     block-type)
        (set-block-header-length!   (+ block-header-size (.limit data)))
        (set-block-header-checksum! (block-checksum header data)))))

(defn write-block!
  "Writes a block to a handle at the given offset, given a block type as a
  keyword and a ByteBuffer for the block's data. Returns handle."
  [handle ^long offset block-type data]
  (-> handle
      (write-block-header! offset (block-header-for-data block-type data))
      (write-block-data!   offset data)))

(defn read-block-by-offset*
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

(declare read-block-index-block
         read-fressian-block
         read-partial-map-block)

(defn read-block-by-offset
  "Takes a Handle and the offset of a block. Reads the block header, validates
  the checksum, and interprets the block data depending on the block type.
  Returns a map of:

    {:type   The block type, as a keyword
     :offset The offset of this block
     :length How many bytes are in this block, total
     :data   The interpreted data stored in this block---depends on block type}"
  [handle offset]
  (prep-read! handle)
  (let [{:keys [header data]} (read-block-by-offset* handle offset)
        type (block-header-type header)]
    {:type   type
     :offset offset
     :length (block-header-length header)
     :data   (case type
               :block-index (read-block-index-block handle data)
               :fressian    (read-fressian-block    handle data)
               :partial-map (read-partial-map-block handle data))}))

;; Block indices

(defn new-block-id!
  "Takes a handle and returns a fresh block ID for that handle, mutating the
  handle so that this ID will not be allocated again."
  [handle]
  (let [index (:block-index handle)
        bs    (:blocks @index)
        id    (int (if (empty? bs)
                     1 ; Blocks start at 1
                     (inc (reduce max (keys bs)))))]
    (swap! index assoc-in [:blocks id] :reserved)
    id))

(defn next-block-offset
  "Takes a handle and returns the offset of the next block. Right now this is
  just the end of the file."
  [handle]
  (max (.size ^FileChannel (:file handle))
       first-block-offset))

(defn assoc-block!
  "Takes a handle, a block ID, and its corresponding offset. Updates the
  handle's block index (in-memory) to add this mapping. Returns handle."
  [handle id offset]
  (swap! (:block-index handle) assoc-in [:blocks id] offset)
  handle)

(defn set-root!
  "Takes a handle and a block ID. Updates the handle's block index (in-memory)
  to point to this block ID as the root. Returns handle."
  [handle root-id]
  (swap! (:block-index handle) assoc :root root-id)
  handle)

(defn block-index-data-size
  "Takes a block index and returns the number of bytes required for that block
  to be written, NOT including headers."
  [index]
  (+ block-id-size
     (* (count (:blocks index))
        (+ block-id-size block-offset-size))))

(defn write-block-index!
  "Writes a block index for a Handle, based on whatever that Handle's current
  block index is. Automatically generates a new block ID for this index and
  adds it to the handle as well. Then writes a new block index offset pointing
  to this block index. Returns handle."
  ([handle]
   (let [id     (new-block-id! handle)
         offset (next-block-offset handle)
         _      (assoc-block! handle id offset)]
     (write-block-index! handle offset)))
  ([handle offset]
   (prep-write! handle)
   (let [file    ^FileChannel (:file handle)
         index   @(:block-index handle)
         data    (ByteBuffer/allocate (block-index-data-size index))]
     ; Write the root ID
     (.putInt data (or (:root index) (int 0)))
     ; And each block mapping
     (doseq [[id offset] (:blocks index)]
       (when-not (= :reserved offset)
         (.putInt data id)
         (.putLong data offset)))
     (.flip data)

     ; Write the header and data to the file.
     (write-block! handle offset :block-index data)
     ; And the block index offset
     (write-block-index-offset! handle offset))
   handle))

(defn read-block-index-block
  "Takes a ByteBuffer and reads a block index from it: a map of

    {:root   root-id
     :blocks {id offset, id2 offset2, ...}}"
  [handle ^ByteBuffer data]
  (let [root (.getInt data)]
    (loop [index (transient {})]
      (if (.hasRemaining data)
        (let [id      (.getInt data)
              offset  (.getLong data)]
          (recur (assoc! index id offset)))
        {:root   (if (zero? root) nil root)
         :blocks (persistent! index)}))))

(defn load-block-index!
  "Takes a handle, reloads its block index from disk, and returns handle."
  [handle]
  (let [block (read-block-by-offset handle (read-block-index-offset handle))]
    (assert+ (= :block-index (:type block))
             {:type     ::block-type-mismatch
              :expected :block-index
              :actual   (:type block)})
    (reset! (:block-index handle) (:data block))
    ;(info :block-index (:data block))
    )
  handle)

(defn read-block-by-id
  "Takes a handle and a logical block id. Assumes that the handle has a
  loaded block id. Looks up the offset for the given block and reads it using
  read-block-by-offset (which includes verifying the checksum)."
  [handle id]
  (assert (instance? Integer id)
          (str "Block ids are integers, not " (class id) " - " (pr-str id)))
  (if-let [offset (get-in @(:block-index handle) [:blocks id])]
    (read-block-by-offset handle offset)
    (throw+ {:type             ::block-not-found
             :id               id
             :known-block-ids (sort (keys (:blocks @(:block-index handle))))})))

(defn read-root
  "Takes a handle. Looks up the root block from the current block index and
  reads it. Returns nil if there is no root."
  [handle]
  (prep-read! handle)
  (when-let [root (:root @(:block-index handle))]
    (read-block-by-id handle root)))

;; Garbage collection

(declare partial-map-rest-id)

(defn block-references
  "Takes a handle and a block ID, and returns the set of all block IDs which
  that block references. Right now we do this by parsing the block data; later
  we might want to move references into block headers. With no block ID,
  returns references from the root."
  ([handle]
   (if-let [root (:root @(:block-index handle))]
     (conj (block-references handle root) root)
     #{}))
  ([handle block-id]
   (let [block (read-block-by-id handle block-id)
         shallow-refs
         (case (:type block)
           :block-index #{}
           :fressian    (let [refs (atom #{})]
                          (walk/postwalk (fn finder [x]
                                           (when (instance? BlockRef x)
                                             (swap! refs conj (:id x)))
                                           x)
                                         (:data block))
                          @refs)
           :partial-map (if-let [id (partial-map-rest-id (:data block))]
                          #{(partial-map-rest-id (:data block))}
                          #{}))]
     (->> shallow-refs
          (map (partial block-references handle))
          (cons shallow-refs)
          (reduce set/union)))))

(defn copy!
  "Takes two handles: a reader and a writer. Copies the root and any other
  referenced blocks from reader to writer."
  [r w]
  (prep-read! r)
  (prep-write! w)
  (when-let [root (:root @(:block-index r))]
    (let [r-file ^FileChannel (:file r)
          w-file ^FileChannel (:file w)
          block-index @(:block-index r)
          ; What blocks are reachable from the root?
          block-ids (block-references r)
          ; Fetch lengths and offsets for those block IDs
          headers (->> block-ids
                       (map (fn get-header [block-id]
                              (let [offset (get-in block-index
                                                   [:blocks block-id])
                                    header (read-block-header r offset)]
                                [block-id
                                 {:length (block-header-length header)
                                  :offset offset}])))
                       (into {}))
          ; Order these: root first, then by size, small blocks first
          block-ids (->> (disj block-ids root)
                         (sort-by (comp :length headers))
                         (cons root))
          ; Work out new block index. Bit of a hack here: we want to write our
          ; block index at the start of the file, which means all the other
          ; offsets depend on it, so we need to know exactly how big it is.
          ; We'll use block-index-size, which doesn't actually care about the
          ; blocks themselves; just how many there are. We add an extra `nil`
          ; block for the block index block itself.
          block-index-size (+ block-header-size
                              (block-index-data-size
                                {:blocks (cons nil block-ids)}))
          ; Now work out the IDS and offsets for our new index
          block-index'
          (loop [; Offset in new file
                 offset       (+ first-block-offset
                                 block-index-size)
                 ; Remaining old block IDs
                 block-ids    block-ids
                 ; New block index
                 block-index' {:blocks {}}]
            (if-not (seq block-ids)
              block-index'
              (let [block-id (first block-ids)
                    length   (:length (get headers block-id))
                    offset'  (+ offset length)]
                (recur (+ offset length)
                       (next block-ids)
                       (cond-> (assoc-in block-index'
                                         [:blocks block-id]
                                         offset)
                         ; If this is the root block, update root too
                         (= root block-id) (assoc :root block-id))))))]
          ; Clobber writer's block index
          (reset! (:block-index w) block-index')
          ; Write block index. Make sure we're actually starting where we think
          ; we are...
          (assert (= first-block-offset (next-block-offset w)))
          (write-block-index! w)
          ; And that we finish at the offset we thought we would...
          (assert+ (= (+ first-block-offset block-index-size)
                        (next-block-offset w))
                     {:type     ::unexpected-block-index-size
                      :expected block-index-size
                      :actual   (- (next-block-offset w) first-block-offset)})

          ; Copy remaining blocks in order
          (.position w-file ^long (+ first-block-offset
                                     block-index-size))
          (doseq [block-id block-ids]
            (let [{:keys [offset length]} (get headers block-id)
                  copied (.transferTo r-file offset length w-file)]
              (when-not (= length copied)
                (throw+ {:type            ::copy-failed
                         :expected-bytes  length
                         :copied-bytes    copied})))))))

(defn gc!
  "Garbage-collects a file (anything that works with io/file) in-place."
  [file]
  (write-atomic! [tmp (io/file file)]
    (with-open [r ^Closeable (open file)
                w ^Closeable (open tmp)]
      (copy! r w))))

;; Fressian blocks

(def fressian-write-handlers
  "How do we write Fressian data?"
  (-> jsf/write-handlers*
      (assoc jepsen.store.format.BlockRef
             {"block-ref" (reify WriteHandler
                            (write [_ w block-ref]
                              (.writeTag w "block-ref" 1)
                              (.writeObject w (:id block-ref))))})
      fress/associative-lookup
      fress/inheritance-lookup))

(def fressian-read-handlers
  "How do we read Fressian data?"
  (-> jsf/read-handlers*
      (assoc "block-ref" (reify ReadHandler
                           (read [_ rdr tag component-count]
                             (block-ref (int (.readObject rdr))))))
      fress/associative-lookup))

(defn write-fressian-to-file!
  "Takes a FileChannel, an offset, a checksum, and a data structure as
  Fressian. Writes the data structure as Fressian to the file at the given
  offset. Returns the size of the data that was just written, in bytes. Mutates
  checksum with written bytes."
  [^FileChannel file, ^long offset, ^CRC32 checksum, data]
  ; First, write the data to the file directly; then we'll go back and write
  ; the header.
  (with-open [foos (FileOffsetOutputStream.
                     file offset checksum)
              bos  (BufferedOutputStream.
                     foos fressian-buffer-size)
              w    ^Closeable (jsf/writer
                                bos {:handlers fressian-write-handlers})]
    (fress/write-object w data)
    (.flush bos)
    (.bytesWritten foos)))

(defn write-fressian-block!*
  "Takes a handle, a byte offset, and some Clojure data. Writes that data to a
  Fressian block at the given offset. Returns handle."
  [handle offset data]
  ; First, write the data to the file directly; then we'll go back and write
  ; the header.
  (let [data-offset (+ offset block-header-size)
        checksum    (CRC32.)
        data-size   (write-fressian-to-file!
                      (:file handle) data-offset checksum data)
        ; Construct a ByteBuffer over the region we just wrote
        data-buf    (read-file (:file handle) data-offset data-size)
        ; And build our header
        header      (block-header-for-length-and-checksum!
                      :fressian data-size checksum)]
    ; Now write the header; data's already in the file.
    (write-block-header! handle offset header)
  handle))

(defn write-fressian-block!
  "Takes a handle and some Clojure data. Writes that data to a Fressian block
  at the end of the file, records the new block in the handle's block index,
  and returns the ID of the newly written block."
  [handle data]
  (let [offset (next-block-offset handle)
        id     (new-block-id! handle)]
    (-> handle
        (write-fressian-block!* offset data)
        (assoc-block! id offset))
    id))

(defn read-fressian-block
  "Takes a handle and a ByteBuffer of data from a Fressian block. Returns its
  parsed contents."
  [handle ^ByteBuffer data]
  (jsf/postprocess-fressian
    (with-open [is (bs/to-input-stream data)
                r  ^Closeable (jsf/reader
                                is {:handlers fressian-read-handlers})]
      (fress/read-object r))))

;; Partial map blocks

(defn write-partial-map-block!*
  "Takes a handle, a byte offset, a Clojure map, and the ID of the block which
  stores the rest of the map (use `nil` if there is no more to the PartialMap).
  Writes the map and rest pointer to a PartialMap block at the given offset.
  Returns handle."
  [handle offset m rest-id]
  (assert (map? m))
  (let [; First, write the rest pointer to the file
        file        (:file handle)
        checksum    (CRC32.)
        rest-buf    (doto (ByteBuffer/allocate block-id-size)
                      (.putInt 0 (if rest-id (int rest-id) 0))
                      .rewind)
        rest-offset (+ offset block-header-size)
        _           (write-file! file rest-offset rest-buf)
        _           (.update checksum (.rewind rest-buf))
        ; Next, stream the map as Fressian to the rest of the block.
        map-size   (write-fressian-to-file!
                     file
                     (+ rest-offset block-id-size)
                     checksum
                     m)
        ; And construct a buffer over the entire block data region
        data-buf    (read-file file rest-offset (+ block-id-size map-size))
        ; And construct our header
        header (block-header-for-length-and-checksum!
                 :partial-map (+ block-id-size map-size) checksum)]
    ; Write header
    (write-block-header! handle offset header))
  handle)

(defn write-partial-map-block!
  "Takes a handle, a Clojure map, and the ID of the block which stores the rest
  of the map (use `nil` if there is no more data to the PartialMap). Writes the
  map to a new PartialMap block, records it in the handle's block index, and
  returns the ID of this block itself. Optionally takes an explicit ID for this
  block."
  ([handle m rest-id]
   (write-partial-map-block! handle (new-block-id! handle) m rest-id))
  ([handle id m rest-id]
   (let [offset (next-block-offset handle)]
     (-> handle
         (write-partial-map-block!* offset m rest-id)
         (assoc-block! id offset))
     id)))

; A lazy map structure which has a map m and a *rest-map*: a Delay which can
; unpack to another PartialMap (presumably by reading another block in the
; file).
(defprotocol IPartialMap
  (partial-map-rest-id [this]))

(def-map-type PartialMap [m rest-id rest-map metadata]
  (get [this key default-value]
       ; Try to retrieve from this map, or from the rest map.
       (if (contains? m key)
         (get m key)
         (get @rest-map key default-value)))

  (assoc [this key value]
         ; We associate onto the top-level map.
         (PartialMap. (assoc m key value) rest-id rest-map metadata))

  (dissoc [this key]
          ; Here we have to strip out the key at every level
          (PartialMap. (dissoc m key)
                       rest-id
                       (delay (dissoc @rest-map key))
                       metadata))

  (keys [this]
        ; We unify keys at all levels recursively.
        (distinct (concat (keys m) (keys @rest-map))))

  (meta [this]
        metadata)

  (with-meta [this metadata']
             (PartialMap. m rest-id rest-map metadata'))

  IPartialMap
  (partial-map-rest-id [this]
    rest-id))

(defn read-partial-map-block
  "Takes a handle and a ByteBuffer for a partial-map block. Returns a lazy map
  representing its data."
  [handle ^ByteBuffer data]
  (let [; Read next ID
        rest-id (.getInt data)
        rest-id (when-not (zero? rest-id) rest-id)
        ; And read map
        m (with-open [is (bs/to-input-stream data)
                      r  ^Closeable (jsf/reader is)]
            (-> (fress/read-object r)
                (jsf/postprocess-fressian)))
        ; Construct a lazy delay for the rest of the partialmap. Zero denotes
        ; there's no more.
        rest-map (delay
                   (when rest-id
                     (let [block (read-block-by-id handle rest-id)]
                       (assert+ (= :partial-map (:type block))
                                {:type      ::block-type-mismatch
                                 :expected  :partial-map
                                 :actual    (:type block)})
                       (:data block))))]
    ; Construct lazy map
    (PartialMap. m rest-id rest-map {})))

;; Test-specific writing

(defn write-initial-test!
  "Writes an initial test to a handle, making the test the root. Called when we
  first begin a test. Returns test."
  [handle test]
  (let [id (write-fressian-block! handle test)]
    (-> handle (set-root! id) write-block-index!))
  test)

(defn write-history!
  "Takes a handle and a test, and appends its :history as a block, then writes
  the test using that history as the new root. Returns test with additional
  metadata, so that future writes of this test can refer to this history
  block."
  [handle test]
  (prep-read! handle)
  (let [history    (:history test)
        history-id (write-fressian-block! handle history)
        test-id    (write-fressian-block!
                     handle (assoc test :history (block-ref history-id)))]
    (-> handle (set-root! test-id) write-block-index!)
    (vary-meta test assoc ::history-id history-id)))

(defn write-results!
  "Takes a handle and a test with a ::history-id metadata key (presumably
  provided by write-history!), and appends its :results as a partial map block:
  :valid? in the top tier, and other results below. Writes test using those
  results and history blocks. Returns test, with ::results-id metadata pointing
  to the block ID of these results."
  [handle test]
  (prep-read! handle)
  (let [history-id  (::history-id (meta test))
        _           (assert+ (integer? history-id)
                             {:type ::no-history-id-in-meta})
        results     (:results test)
        more-id     (write-partial-map-block!
                      handle (dissoc results :valid?) nil)
        results-id  (write-partial-map-block!
                      handle (select-keys results [:valid?]) more-id)
        test-id     (write-fressian-block!
                      handle
                      (assoc test
                             :history (block-ref history-id)
                             :results (block-ref results-id)))]
    (-> handle (set-root! test-id) write-block-index!)
    (vary-meta test assoc ::results-id results-id)))

(defn write-test!
  "Writes an entire test map to a handle, making the test the root. Useful for
  re-writing a completed test that's already in memory. Returns handle."
  [handle test]
  (let [; Where will we store the remainder of the :results field?
        more-results-id (new-block-id! handle)

        ; Write the minimal part of the results
        results (:results test)
        results-id (write-partial-map-block! handle
                                             (select-keys results [:valid?])
                                             more-results-id)
        ; And the rest of the results
        _ (write-partial-map-block! handle
                                    more-results-id
                                    (dissoc results :valid?)
                                    nil)
        ; Next, the history
        history-id (write-fressian-block! handle (:history test))

        ; And finally, the test
        test    (assoc test
                       :history (block-ref history-id)
                       :results (block-ref results-id))
        test-id (write-fressian-block! handle test)]
    (-> handle
        (set-root! test-id)
        write-block-index!)))

(def-map-type LazyTest [m history results metadata]
  (get [this key default-value]
       (condp identical? key
         :history (if history @history default-value)
         :results (if results @results default-value)
         (get m key default-value)))

  (assoc [this key value]
         (condp identical? key
           :history (LazyTest. m (deliver (promise) value) results metadata)
           :results (LazyTest. m history (deliver (promise) value) metadata)
           (LazyTest. (assoc m key value) history results metadata)))

  (dissoc [this key]
          (condp identical? key
            :history (LazyTest. m nil results metadata)
            :results (LazyTest. m history nil metadata)
            (LazyTest. (dissoc m key) history results metadata)))

  (keys [this]
        (cond-> (keys m)
          history (conj :history)
          results (conj :results)))

  (meta [this]
        metadata)

  (with-meta [this metadata']
             (LazyTest. m history results metadata')))

(defn read-test
  "Reads a test from a handle's root. Constructs a lazy test map where history
  and results are loaded as-needed from the file. Leave the handle open so this
  map can use it; it'll be automatically closed when this map is GCed. Includes
  metadata so that this test can be rewritten using write-results!"
  [handle]
  (let [test    (:data (read-root handle))
        h       (:history test)
        r       (:results test)
        history (when h
                  (delay (:data (read-block-by-id handle (:id h)))))
        results (when r
                  (delay (:data (read-block-by-id handle (:id r)))))]
    (LazyTest. (dissoc test :history :results)
               history
               results
               {::history-id (:id h)
                ::results-id (:id r)})))
