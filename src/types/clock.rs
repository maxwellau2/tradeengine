use quanta::Clock;
use std::sync::OnceLock;

static GLOBAL_CLOCK: OnceLock<Clock> = OnceLock::new();

pub fn global_clock() -> &'static Clock {
    GLOBAL_CLOCK.get_or_init(|| Clock::new())
}

#[inline]
pub fn timestamp_nanos() -> u64 {
    coarsetime::Clock::now_since_epoch().as_nanos() as u64
}

/// high precision timestamp using quanta (rdtsc on x86)
#[inline]
pub fn timestamp_nanos_precise() -> u64 {
    global_clock().raw()
}

#[inline]
pub fn timestamp_micros() -> u64 {
    coarsetime::Clock::now_since_epoch().as_micros()
}

#[inline]
pub fn timestamp_millis() -> u64 {
    coarsetime::Clock::now_since_epoch().as_millis()
}

mod tests {
    use super::*;
    #[test]
    fn test_sequence_number_wraparound() {
        let mut seq = u64::MAX - 1;
        seq = seq.wrapping_add(1);
        assert_eq!(seq, u64::MAX);
        seq = seq.wrapping_add(1);
        assert_eq!(seq, 0); // Wraps around
    }

    #[test]
    fn test_timestamp_nanos_reasonable() {
        let ts = timestamp_nanos();
        // Should be roughly current time (2025 = ~1.7e18 ns since epoch)
        assert!(ts > 1_700_000_000_000_000_000);
        assert!(ts < 2_000_000_000_000_000_000);
    }

    #[test]
    fn test_timestamp_monotonic() {
        let ts1 = timestamp_nanos();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let ts2 = timestamp_nanos();
        assert!(ts2 >= ts1);
    }
}
