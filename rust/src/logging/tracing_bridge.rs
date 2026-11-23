/*
 * This file is licensed under the Affero General Public License (AGPL) version 3.
 *
 * Copyright (C) 2026 Element Creations Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * See the GNU Affero General Public License for more details:
 * <https://www.gnu.org/licenses/agpl-3.0.html>.
 *
 */

//! Forwards `tracing` events (e.g. Rezzy's reconciliation diagnostics) into the
//! `log` crate, and from there into Python logging via `pyo3-log`.
//!
//! Severity is preserved and the event's `target` becomes the Python logger
//! name. Whether an event is emitted is decided by the Python logging
//! configuration: [`Subscriber::enabled`] defers to `log`'s max level and the
//! installed logger's `enabled`, which `pyo3-log` answers from the Python
//! logger's effective level.
//!
//! Spans are not recorded; only events are forwarded.

use std::fmt::{Debug, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

struct LogBridge;

fn to_log_level(level: &Level) -> log::Level {
    match *level {
        Level::ERROR => log::Level::Error,
        Level::WARN => log::Level::Warn,
        Level::INFO => log::Level::Info,
        Level::DEBUG => log::Level::Debug,
        Level::TRACE => log::Level::Trace,
    }
}

#[derive(Default)]
struct Fields {
    message: String,
    extra: String,
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.extra, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.extra, " {}={value:?}", field.name());
        }
    }
}

impl Subscriber for LogBridge {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        let level = to_log_level(metadata.level());
        level <= log::max_level()
            && log::logger().enabled(
                &log::Metadata::builder()
                    .level(level)
                    .target(metadata.target())
                    .build(),
            )
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        log::logger().log(
            &log::Record::builder()
                .level(to_log_level(metadata.level()))
                .target(metadata.target())
                .module_path(metadata.module_path())
                .file(metadata.file())
                .line(metadata.line())
                .args(format_args!("{}{}", fields.message, fields.extra))
                .build(),
        );
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Install the bridge as the global `tracing` subscriber. Call after the `log`
/// logger (`pyo3_log::init`) is set up. A subscriber installed earlier wins, and
/// repeated calls are ignored.
pub fn install() {
    let _ = tracing::subscriber::set_global_default(LogBridge);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_map_one_to_one() {
        assert_eq!(to_log_level(&Level::ERROR), log::Level::Error);
        assert_eq!(to_log_level(&Level::WARN), log::Level::Warn);
        assert_eq!(to_log_level(&Level::INFO), log::Level::Info);
        assert_eq!(to_log_level(&Level::DEBUG), log::Level::Debug);
        assert_eq!(to_log_level(&Level::TRACE), log::Level::Trace);
    }
}
