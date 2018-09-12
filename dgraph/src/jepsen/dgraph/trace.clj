(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span)
           (io.opencensus.trace.samplers Samplers)
           (io.opencensus.exporter.trace.logging LoggingTraceExporter)))

(def tracer ^Tracer (Tracing/getTracer))
(def trace-exporter (LoggingTraceExporter/register))

;; TODO Set a higher sample rate
#_(def trace-config
  (let [sampler (Samplers/alwaysSample)
        config  (Tracing/getTraceConfig)
        config' (->> config
                    .getActiveTraceParams
                    .toBuilder
                    (.setSampler sampler)
                    .build)]
    (.updateActiveTraceParams config')))

(defmacro with-trace
  "Takes a span name and a body and uses this namespace's tracer to
  wrap the body in a span."
  [name & body]
  `(let [span# (-> tracer (.spanBuilder ~name) .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))
