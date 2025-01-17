(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span
                                AttributeValue)
           (io.opencensus.trace.samplers Samplers)
           (io.opencensus.exporter.trace.jaeger JaegerTraceExporter)))

(defn sampler
  "Enables sampling if a tracing service is provided."
  [enable?]
  (if enable?
    (Samplers/alwaysSample)
    (Samplers/neverSample)))

(defn config [sampler]
  (let [config (Tracing/getTraceConfig)
        params (-> config
                   .getActiveTraceParams
                   .toBuilder
                   (.setSampler sampler)
                   .build)]
    (.updateActiveTraceParams config params)
    true))

(defn exporter
  "When tracing is enabled, registers an exporter to the given jaeger
  service."
  [endpoint]
  (when endpoint
    (try
      (JaegerTraceExporter/createAndRegister endpoint "jepsen")
      (catch java.lang.IllegalStateException _))))

(defn tracing [endpoint]
  (let [sampler (sampler endpoint)]
    {:endpoint endpoint
     :config (config sampler)
     :exporter (exporter endpoint)}))

(defmacro with-trace
  "Takes a span name, and a body and wraps the
  body in a tracing span."
  [name & body]
  `(let [span# (-> (Tracing/getTracer)
                   (.spanBuilder ~name)
                   .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))

(defn context
  "Returns the context map for the current trace."
  []
  (let [span (.getCurrentSpan (Tracing/getTracer))
        context (.getContext span)]
    {:span-id  (-> context .getSpanId .toString)
     :trace-id (-> context .getTraceId .toString)}))

(defn annotate!
  "Annotates the current span with the message."
  [message]
  (let [span (.getCurrentSpan (Tracing/getTracer))]
    (.addAnnotation span message)))

(defn attribute!
  "Takes a key and value, or a map of keys to values, and assigns the kv
  pairs as attributes on the current span. All keys and values MUST be strings
  or opencensus will throw."
  [k v]
  (let [span (.getCurrentSpan (Tracing/getTracer))
        val  (AttributeValue/stringAttributeValue v)]
    (.putAttribute span k val)))
