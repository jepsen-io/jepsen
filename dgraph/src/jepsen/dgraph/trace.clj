(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span)
           (io.opencensus.trace.samplers Samplers)
           (io.opencensus.exporter.trace.logging LoggingTraceExporter)
           (io.opencensus.exporter.trace.jaeger JaegerTraceExporter)))

(def tracer         (atom nil))
(def trace-exporter (atom nil))
(def sampler        (atom nil))
(def trace-config   (atom nil))

(defn get-trace-config []
  (let [config (Tracing/getTraceConfig)
        params (-> config
                   .getActiveTraceParams
                   .toBuilder
                   (.setSampler @sampler)
                   .build)]
    (.updateActiveTraceParams config params)))

(defn init-tracing!
  [{:keys [tracing]}]
  (reset! tracer         (Tracing/getTracer))

  ;; Connect to the jaeger service to export spans
  (reset! trace-exporter (when tracing
                           (JaegerTraceExporter/createAndRegister tracing "jepsen")))

  ;; If there's nowhere to send traces don't trace
  (reset! sampler        (if tracing
                           (Samplers/alwaysSample)
                           (Samplers/neverSample)))
  (reset! trace-config   (get-trace-config)))

(defmacro with-trace
  "Takes a span name and a body and uses this namespace's tracer to
  wrap the body in a span."
  [name & body]
  `(let [span# (-> @tracer (.spanBuilder ~name) .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))

(defn context []
  (let [span (.getCurrentSpan @tracer)
        context (.getContext span)]
    {:span-id  (-> context .getSpanId .toString)
     :trace-id (-> context .getTraceId .toString)}))
