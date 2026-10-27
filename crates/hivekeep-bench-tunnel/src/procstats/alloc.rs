//! A global allocator that counts the bytes in use, standing in for a
//! garbage-collected runtime's heap figures. Rust frees memory as soon as it
//! is dropped, so the count is what is live.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

static IN_USE: AtomicU64 = AtomicU64::new(0);
static INSTALLED: AtomicU64 = AtomicU64::new(0);

/// The system allocator, counting bytes allocated and not yet freed.
/// Install with `#[global_allocator]`; it costs two relaxed atomic adds per
/// allocation.
pub struct CountingAllocator;

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counters do not affect what is allocated.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            IN_USE.fetch_add(layout.size() as u64, Ordering::Relaxed);
            INSTALLED.store(1, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's guarantees.
        unsafe { System.dealloc(ptr, layout) };
        IN_USE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            IN_USE.fetch_add(layout.size() as u64, Ordering::Relaxed);
            INSTALLED.store(1, Ordering::Relaxed);
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            IN_USE.fetch_add(new_size as u64, Ordering::Relaxed);
            IN_USE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        }
        p
    }
}

/// Heap bytes in use, or zero when [`CountingAllocator`] is not installed.
pub fn heap_in_use() -> u64 {
    if INSTALLED.load(Ordering::Relaxed) == 0 {
        return 0;
    }
    IN_USE.load(Ordering::Relaxed)
}
