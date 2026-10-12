from __future__ import annotations

from typing import Any

from opentracing import Tracer as OpenTracingTracer

from .reporter import BaseReporter
from .span import Span

class Tracer(OpenTracingTracer):
    active_span: Span | None

class Sampler: ...

class ConstSampler(Sampler):
    def __init__(self, decision: bool) -> None: ...

class Config:
    config: dict[str, Any]
    service_name: str | None
    validate: bool
    metrics: Any
    metrics_factory: Any
    scope_manager: Any
    sampler: Any

    def __init__(
        self,
        config: dict[str, Any] | Any,
        service_name: str | None = ...,
        validate: bool = ...,
        metrics: Any = ...,
        metrics_factory: Any = ...,
        scope_manager: Any = ...,
    ) -> None: ...
    def create_tracer(
        self,
        reporter: BaseReporter | Any,
        sampler: Sampler | Any,
        throttler: Any = ...,
    ) -> Tracer: ...
    def initialize_tracer(self, io_loop: Any = ...) -> Tracer | None: ...
