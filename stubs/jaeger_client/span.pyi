from __future__ import annotations

from opentracing import Span as OpenTracingSpan

class Span(OpenTracingSpan):
    start_time: float | None
    end_time: float | None
