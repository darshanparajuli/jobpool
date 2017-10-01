//! A simple and lightweight threadpool implementation.
//!
//! # Getting started
//!
//! Add the following under `[dependencies]` on `Cargo.toml`:
//!
//! ```toml
//! jobpool = "*" # or a specific version from crates.io
//! ```
//!
//! Add the following to the root crate:
//!
//! ```rust
//! extern crate jobpool;
//! ```
//!
//! # Usage
//!
//! ```rust
//! use jobpool::JobPool;
//!
//! let pool_size: usize = 8; // number of cpu cores is recommended
//! let mut pool = JobPool::new(pool_size);
//! // pool.auto_grow(100);
//!
//! for _ in 0..1000 {
//!     pool.queue(|| {
//!         // do some work
//!     });
//! }
//! // ...
//! pool.shutdown(); // waits for jobs to finish
//! ```

mod jobpool;

pub use jobpool::JobPool;
pub use jobpool::Runnable;

#[cfg(test)]
#[allow(unused)]
mod tests {
    use JobPool;
    use std::time::Duration;
    use std::thread;
    use std::sync::{Arc, Mutex, Condvar};

    #[test]
    fn shuts_down() {
        let mut pool = JobPool::new(10);

        let pair = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..100 {
            let pair2 = pair.clone();
            pool.queue(move || {
                let &(ref mutex, ref cvar) = &*pair2;
                let mut guard = mutex.lock().unwrap();
                while !*guard {
                    guard = cvar.wait(guard).unwrap();
                }
            });
        }

        thread::sleep(Duration::from_millis(500));

        assert_eq!(pool.active_workers_count(), 10);

        let &(ref mutex, ref cvar) = &*pair;
        let mut guard = mutex.lock().unwrap();
        *guard = true;
        cvar.notify_all();
        drop(guard);

        pool.shutdown();
    }

    #[test]
    fn shuts_down_with_auto_grow() {
        let mut pool = JobPool::new(8);
        pool.auto_grow(100);

        let pair = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..100 {
            let pair2 = pair.clone();
            pool.queue(move || {
                let &(ref mutex, ref cvar) = &*pair2;
                let mut guard = mutex.lock().unwrap();
                while !*guard {
                    guard = cvar.wait(guard).unwrap();
                }
            });
        }

        thread::sleep(Duration::from_millis(500));

        assert!(pool.active_workers_count() > 8);

        let &(ref mutex, ref cvar) = &*pair;
        let mut guard = mutex.lock().unwrap();
        *guard = true;
        cvar.notify_all();
        drop(guard);

        pool.shutdown();
    }

    #[test]
    #[should_panic]
    fn panic_on_reuse() {
        let mut pool = JobPool::new(10);
        for _ in 0..100 {
            pool.queue(|| {
                // fake work
                thread::sleep(Duration::from_millis(10));
            });
        }
        pool.shutdown();
        pool.queue(|| { let a = 1 + 2; });
    }

    #[test]
    fn no_panic_on_multiple_shutdowns() {
        let mut pool = JobPool::new(10);
        for _ in 0..100 {
            pool.queue(|| {
                // fake work
                thread::sleep(Duration::from_millis(10));
            });
        }
        for _ in 0..10 {
            pool.shutdown();
        }
    }

    #[test]
    #[should_panic]
    fn panic_on_zero_sized_jobpool() {
        let mut pool = JobPool::new(0);
        pool.shutdown();
    }

    #[test]
    fn shutdown_no_wait() {
        let mut pool = JobPool::new(8);
        for _ in 0..100 {
            pool.queue(|| {
                // fake work
                thread::sleep(Duration::from_millis(10));
            });
        }
        let handles = pool.shutdown_no_wait();
        assert!(handles.is_some());
        assert_eq!(handles.unwrap().len(), 8);
    }

    #[test]
    fn shutdown_no_wait_with_auto_grow() {
        let mut pool = JobPool::new(8);
        pool.auto_grow(100);

        let pair = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..100 {
            let pair2 = pair.clone();
            pool.queue(move || {
                let &(ref mutex, ref cvar) = &*pair2;
                let mut guard = mutex.lock().unwrap();
                while !*guard {
                    guard = cvar.wait(guard).unwrap();
                }
            });
        }

        thread::sleep(Duration::from_millis(500));

        assert!(pool.active_workers_count() > 8);

        let &(ref mutex, ref cvar) = &*pair;
        let mut guard = mutex.lock().unwrap();
        *guard = true;
        cvar.notify_all();
        drop(guard);

        let handles = pool.shutdown_no_wait();
        assert!(handles.is_some());

        let handles = handles.unwrap();

        for handle in handles {
            let _ = handle.join();
        }
    }

    #[test]
    fn shouldnt_auto_grow() {
        let mut pool = JobPool::new(10);
        pool.auto_grow(100);

        let pair = Arc::new((Mutex::new(false), Condvar::new()));

        for _ in 0..10 {
            let pair2 = pair.clone();
            pool.queue(move || {
                let &(ref mutex, ref cvar) = &*pair2;
                let mut guard = mutex.lock().unwrap();
                while !*guard {
                    guard = cvar.wait(guard).unwrap();
                }
            });
        }

        thread::sleep(Duration::from_millis(500));

        assert_eq!(pool.active_workers_count(), 10);

        let &(ref mutex, ref cvar) = &*pair;
        let mut guard = mutex.lock().unwrap();
        *guard = true;
        cvar.notify_all();
        drop(guard);

        pool.shutdown();
    }
}
