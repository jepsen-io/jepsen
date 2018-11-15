(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span)
           (io.opencensus.trace.samplers Samplers)
           (io.opencensus.exporter.trace.logging LoggingTraceExporter)
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
    (.updateActiveTraceParams config params)))

(defn exporter
  "When tracing is enabled, registers an exporter to the given jaeger
  service."
  [endpoint]
  (when endpoint
    (JaegerTraceExporter/createAndRegister endpoint "jepsen")))

(defn tracing [endpoint]
  (let [sampler (sampler endpoint)]
    {:endpoint endpoint
     :sampler sampler
     :config (config sampler)
     :exporter (exporter endpoint)}))

(defmacro with-trace
  "Takes a test map, span name, and a body and wraps the
  body in a tracing span."
  [name & body]
  `(let [span# (-> (Tracing/getTracer)
                   (.spanBuilder ~name)
                   .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))

(defn context
  "Takes a test map and returns the context map for the current trace."
  []
  (let [span (.getCurrentSpan (Tracing/getTracer))
        context (.getContext span)]
    {:span-id  (-> context .getSpanId .toString)
     :trace-id (-> context .getTraceId .toString)}))
