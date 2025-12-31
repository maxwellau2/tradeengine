use std::fmt;
use std::sync::Arc;

use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

use super::state::SharedTUIState;

/// custom tracing layer that captures logs to TUI state
pub struct TUILogLayer {
    state: SharedTUIState,
}

impl TUILogLayer {
    pub fn new(state: SharedTUIState) -> Self {
        Self { state }
    }
}

impl<S: Subscriber> Layer<S> for TUILogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = LogVisitor::default();
        event.record(&mut visitor);

        let meta = event.metadata();
        let level = meta.level();
        let file = meta.file().unwrap_or("unknown");
        let line = meta.line().unwrap_or(0);
        let msg = format!("{} {}:{} {}", level, file, line, visitor.message);

        // lock-free update: load current state, add log, store new state
        let current = self.state.load();
        let mut new_state = (**current).clone();
        new_state.logs.push(msg);
        if new_state.logs.len() > 100 {
            new_state.logs.remove(0);
        }
        self.state.store(Arc::new(new_state));
    }
}

#[derive(Default)]
struct LogVisitor {
    message: String,
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            self.message
                .push_str(&format!("{}={:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            self.message
                .push_str(&format!("{}={}", field.name(), value));
        }
    }
}
