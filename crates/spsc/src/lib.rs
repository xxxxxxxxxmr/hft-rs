use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Bounded SPSC ring buffer
pub struct Ring<T, const N: usize> {
    buf: [UnsafeCell<Option<T>>; N],
    head: AtomicUsize,
    tail: AtomicUsize,
}

pub struct Producer<T, const N: usize> {
    ring: Arc<Ring<T, N>>,
}

pub struct Consumer<T, const N: usize> {
    ring: Arc<Ring<T, N>>,
}

impl<T, const N: usize> Clone for Producer<T, N> {
    fn clone(&self) -> Self {
        Self {
            ring: Arc::clone(&self.ring),
        }
    }
}

impl<T, const N: usize> Clone for Consumer<T, N> {
    fn clone(&self) -> Self {
        Self {
            ring: Arc::clone(&self.ring),
        }
    }
}

pub fn channel<T, const N: usize>() -> (Producer<T, N>, Consumer<T, N>) {
    let ring = Arc::new(Ring::new());
    let prod = Producer {
        ring: ring.clone(),
    };
    let cons = Consumer { ring };
    (prod, cons)
}

impl<T, const N: usize> Producer<T, N> {
    #[inline]
    pub fn push(&self, val: T) -> bool {
        self.ring.push(val)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }
}

impl<T, const N: usize> Consumer<T, N> {
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.ring.pop()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }
}

unsafe impl<T: Send, const N: usize> Send for Ring<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}

impl<T, const N: usize> Ring<T, N> {
    pub fn new() -> Self {
        let buf = std::array::from_fn(|_| UnsafeCell::new(None));
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

    #[inline]
    fn len(&self) -> usize {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        h - t
    }
}
