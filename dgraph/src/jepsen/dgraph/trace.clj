(ns jepsen.dgraph.trace
  (:import (io.opencensus.trace Tracer
                                Tracing
                                Span)
           (io.opencensus.exporter.trace.logging LoggingTraceExporter)))

;; Ok so, this is entirely done with globals, which skeevs me out
;; cause I haven't been able to guarantee that a tracer can be global. BUT
;; the docs and examples seeeeeem to imply that it is. Also there's
;; HOPEFULLY everything works hard to tell without seeing exports
;; *shakes fist at LoggingTraceExporter* Anyway, if it does work this is
;; a HELL of a lot simpler than having to pass it tracers everywhere.

(def tracer ^Tracer (Tracing/getTracer))
;; TODO Why the hecky isn't this logging??
(def trace-exporter (LoggingTraceExporter/register))

;; TODO TraceConfig

(defmacro with-trace
  "Takes a span name and a body and uses this namespace's tracer to
  wrap the body in a span."
  [name & body]
  `(let [span# (-> tracer (.spanBuilder ~name) .startScopedSpan)]
     (try
       ~@body
       (finally (.close span#)))))
