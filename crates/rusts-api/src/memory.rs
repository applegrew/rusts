//! Process memory guard for enforcing configurable memory limits.
//!
//! `MemoryGuard` periodically samples the process RSS and exposes
//! `check()` which returns the current memory pressure level.
//! The write handler uses this to apply backpressure:
//!
//! - **Normal** — below soft limit, writes proceed normally.
//! - **High** — above soft limit (90%), force a memtable flush before accepting the write.
//! - **Critical** — above hard limit, reject writes with HTTP 503.

use std::sync::atomic::{AtomicU64, Ordering};

/// Memory pressure level returned by [`MemoryGuard::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Below soft limit — normal operation.
    Normal,
    /// Above soft limit (90% of max) — flush memtables proactively.
    High,
    /// Above hard limit — reject writes.
    Critical,
}

/// Guards against unbounded memory growth by checking process RSS
/// against a configured limit.
///
/// When `max_bytes` is 0 the guard is disabled and `check()` always
/// returns `Normal`.
pub struct MemoryGuard {
    /// Hard limit in bytes (0 = disabled).
    max_bytes: u64,
    /// Soft limit in bytes (90% of max).
    soft_bytes: u64,
    /// Cached RSS value (updated by `refresh()`).
    cached_rss: AtomicU64,
}

impl MemoryGuard {
    /// Create a new guard.  `max_mb == 0` disables the guard.
    pub fn new(max_mb: u64) -> Self {
        let max_bytes = max_mb * 1024 * 1024;
        let soft_bytes = max_bytes * 9 / 10; // 90%
        let rss = get_current_rss_bytes();
        Self {
            max_bytes,
            soft_bytes,
            cached_rss: AtomicU64::new(rss),
        }
    }

    /// Refresh the cached RSS value from the OS.
    pub fn refresh(&self) {
        let rss = get_current_rss_bytes();
        self.cached_rss.store(rss, Ordering::Relaxed);
    }

    /// Check current memory pressure using the cached RSS.
    pub fn check(&self) -> MemoryPressure {
        if self.max_bytes == 0 {
            return MemoryPressure::Normal;
        }
        let rss = self.cached_rss.load(Ordering::Relaxed);
        if rss >= self.max_bytes {
            MemoryPressure::Critical
        } else if rss >= self.soft_bytes {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        }
    }

    /// Returns the current cached RSS in bytes.
    pub fn current_rss_bytes(&self) -> u64 {
        self.cached_rss.load(Ordering::Relaxed)
    }

    /// Returns the configured hard limit in bytes (0 = disabled).
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Returns true if the guard is enabled (max > 0).
    pub fn is_enabled(&self) -> bool {
        self.max_bytes > 0
    }
}

// ---------------------------------------------------------------------------
// Platform-specific current RSS
// ---------------------------------------------------------------------------

/// Returns the **current** resident set size of this process in bytes.
///
/// On macOS we use `task_info(MACH_TASK_BASIC_INFO)` which gives the
/// live RSS (unlike `getrusage` which returns peak RSS).
/// On Linux we read `/proc/self/statm`.
#[cfg(target_os = "macos")]
fn get_current_rss_bytes() -> u64 {
    use std::mem;

    // mach_task_basic_info struct layout
    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [u32; 2],   // time_value_t
        system_time: [u32; 2], // time_value_t
        policy: i32,
        suspend_count: i32,
    }

    const MACH_TASK_BASIC_INFO: u32 = 20;
    const MACH_TASK_BASIC_INFO_COUNT: u32 =
        (mem::size_of::<MachTaskBasicInfo>() / mem::size_of::<u32>()) as u32;

    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: u32,
            task_info_out: *mut MachTaskBasicInfo,
            task_info_out_cnt: *mut u32,
        ) -> i32;
    }

    unsafe {
        let mut info: MachTaskBasicInfo = mem::zeroed();
        let mut count = MACH_TASK_BASIC_INFO_COUNT;
        let kr = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info as *mut _,
            &mut count,
        );
        if kr == 0 {
            info.resident_size
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn get_current_rss_bytes() -> u64 {
    // /proc/self/statm — field 1 is RSS in pages
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(rss_pages) = statm.split_whitespace().nth(1) {
            if let Ok(pages) = rss_pages.parse::<u64>() {
                let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
                return pages * page_size;
            }
        }
    }
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_current_rss_bytes() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_guard_always_normal() {
        let guard = MemoryGuard::new(0);
        assert!(!guard.is_enabled());
        assert_eq!(guard.check(), MemoryPressure::Normal);
    }

    #[test]
    fn rss_is_nonzero() {
        let rss = get_current_rss_bytes();
        // The test process itself should use some memory
        assert!(rss > 0, "expected nonzero RSS, got {}", rss);
    }

    #[test]
    fn guard_with_huge_limit_is_normal() {
        let guard = MemoryGuard::new(1024 * 1024); // 1 TB
        assert!(guard.is_enabled());
        assert_eq!(guard.check(), MemoryPressure::Normal);
    }

    #[test]
    fn guard_with_tiny_limit_is_critical() {
        let guard = MemoryGuard::new(1); // 1 MB — any real process exceeds this
        assert_eq!(guard.check(), MemoryPressure::Critical);
    }
}
