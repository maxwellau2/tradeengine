use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::types::common::{Balance, Order, Position};

/// global shutdown flag
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub fn request_shutdown() {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

pub fn should_shutdown() -> bool {
    SHUTDOWN.load(Ordering::SeqCst)
}

/// shared state between central and TUI
#[derive(Debug, Clone, Default)]
pub struct TUIState {
    pub orders: Vec<Order>,
    pub positions: Vec<Position>,
    pub balances: Vec<Balance>,
    pub logs: Vec<String>,
}

const MAX_LOG_LINES: usize = 100;

impl TUIState {
    pub fn new() -> Self {
        Self::default()
    }

    /// add log and return new state (for immutable update pattern)
    pub fn with_log(mut self, msg: String) -> Self {
        self.logs.push(msg);
        if self.logs.len() > MAX_LOG_LINES {
            self.logs.remove(0);
        }
        self
    }
}

/// thread-safe handle to TUI state using ArcSwap for lock-free reads
/// - Central calls store() to atomically swap in new state
/// - TUI calls load() to get current state without blocking
pub type SharedTUIState = Arc<ArcSwap<TUIState>>;

pub fn new_shared_state() -> SharedTUIState {
    Arc::new(ArcSwap::from_pointee(TUIState::new()))
}
