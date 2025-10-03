use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bounded SPSC ring buffer
pub struct Ring<T, const N: usize> {
    buf: [UnsafeCell<Option<T>>; N],
    head: AtomicUsize,
    tail: AtomicUsize,
}

unsafe impl<T: Send, const N: usize> Send for Ring<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}

impl<T, const N: usize> Ring<T, N> {
    pub const fn new() -> Self {
        const NONE: UnsafeCell<Option<()>> = UnsafeCell::new(None);
        let buf = [NONE; N];
        let buf = unsafe { std::mem::transmute::<_, [UnsafeCell<Option<T>>; N]>(buf) };
        Self {
            buf,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, val: T) -> bool {
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Acquire);
        if h - t == N {
            return false; // full
        }
        unsafe { *self.buf[h % N].get() = Some(val); }
        self.head.store(h + 1, Ordering::Release);
        true
    }

    pub fn pop(&self) -> Option<T> {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Acquire);
        if t == h {
            return None;
        }
        let val = unsafe { (*self.buf[t % N].get()).take() };
        self.tail.store(t + 1, Ordering::Release);
        val
    }
}