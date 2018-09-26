(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span)
           (io.opencensus.trace.samplers Samplers)
           (io.opencensus.exporter.trace.logging LoggingTraceExporter)))

(def tracer ^Tracer (Tracing/getTracer))
(def trace-exporter (LoggingTraceExporter/register))

;; TODO Add a CLI input for choosing between samplers
(def sampler ^Sampler
  #_(Samplers/alwaysSample)
  (Samplers/neverSample))

(def trace-config
  (let [config (Tracing/getTraceConfig)
        params (-> config
                   .getActiveTraceParams
                   .toBuilder
                   (.setSampler sampler)
                   .build)]
    (.updateActiveTraceParams config params)))

(defmacro with-trace
  "Takes a span name and a body and uses this namespace's tracer to
  wrap the body in a span."
  [name & body]
  `(let [span# (-> tracer (.spanBuilder ~name) .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))
