//! cpu core pinning utilities for low-latency trading
//!
//! provides functions to pin threads to specific cpu cores, which is critical
//! for hft systems to avoid scheduler jitter and cache thrashing.

use core_affinity::CoreId;
use tracing::{error, info};

/// pin current thread to specified core
///
/// panics if pinning fails - this is intentional since core pinning is
/// mandatory for our hft architecture. if a core is unavailable, we want
/// to fail fast rather than run with unpredictable latency.
pub fn pin_to_core(core_id: usize) {
    let core = CoreId { id: core_id };
    if core_affinity::set_for_current(core) {
        info!("pinned thread to core {}", core_id);
    } else {
        panic!(
            "failed to pin thread to core {} - check core availability",
            core_id
        );
    }
}

/// pin current thread to specified core, returns result instead of panicking
///
/// use this variant when you want to handle pinning failure gracefully
pub fn try_pin_to_core(core_id: usize) -> Result<(), CorePinError> {
    let core = CoreId { id: core_id };
    if core_affinity::set_for_current(core) {
        info!("pinned thread to core {}", core_id);
        Ok(())
    } else {
        error!("failed to pin thread to core {}", core_id);
        Err(CorePinError::PinFailed(core_id))
    }
}

/// get available core count on this system
pub fn available_cores() -> usize {
    core_affinity::get_core_ids()
        .map(|ids| ids.len())
        .unwrap_or(1)
}

/// validate that requested cores are available
pub fn validate_cores(main_core: usize, io_core: usize) -> Result<(), CorePinError> {
    let available = available_cores();
    if main_core >= available {
        return Err(CorePinError::CoreUnavailable {
            requested: main_core,
            available,
        });
    }
    if io_core >= available {
        return Err(CorePinError::CoreUnavailable {
            requested: io_core,
            available,
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CorePinError {
    #[error("failed to pin thread to core {0}")]
    PinFailed(usize),

    #[error("core {requested} unavailable, system has {available} cores")]
    CoreUnavailable { requested: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_cores() {
        let cores = available_cores();
        assert!(cores >= 1, "should have at least 1 core");
        println!("system has {} cores", cores);
    }

    #[test]
    fn test_validate_cores_success() {
        let cores = available_cores();
        if cores >= 2 {
            assert!(validate_cores(0, 1).is_ok());
        }
    }

    #[test]
    fn test_validate_cores_failure() {
        let cores = available_cores();
        let result = validate_cores(cores + 10, 0);
        assert!(result.is_err());
    }
}
