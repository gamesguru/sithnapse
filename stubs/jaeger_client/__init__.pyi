from __future__ import annotations

from .config import Config, Tracer
from .span import Span
from .span_context import SpanContext

class ConstSampler:
    def __init__(self, decision: bool) -> None: ...

__all__ = [
    "Config",
    "ConstSampler",
    "Span",
    "SpanContext",
    "Tracer",
]
