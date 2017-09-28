//! A simple, lightweight, and fixed-size threadpool API.
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
//! pool.queue(|| {
//!     // do some work
//! });
//! // ...
//! pool.shutdown(); // waits for jobs to finish
//! ```

#![warn(missing_docs)]

use std::sync::{Arc, Condvar, Mutex};
use std::{process, thread};
use std::collections::VecDeque;

type BoxedJob = Box<Runnable + Send + 'static>;

/// A trait for giving a type an ability to run some code.
pub trait Runnable {
    /// Runs some code.
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Runnable for F {
    #[inline]
    fn run(self: Box<F>) {
        (*self)()
    }
}

struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, work_queue: Arc<Mutex<VecDeque<Option<BoxedJob>>>>, condvar: Arc<Condvar>) -> Self {
        let builder = thread::Builder::new().name(format!("worker-{}", id));
        let handle = builder.spawn(move || loop {
            let mut guard = work_queue.lock().unwrap();
            while guard.is_empty() {
                // println!("[worker-{}] waiting...", id);
                guard = condvar.wait(guard).unwrap();
                // println!("[worker-{}] notified", id);
            }

            // queue is not empty at this point, so unwrap() is safe
            let work: Option<BoxedJob> = guard.pop_front().unwrap();
            drop(guard);

            match work {
                Some(job) => {
                    // println!("[worker-{}] running job", id);
                    job.run();
                }
                None => {
                    // println!("[worker-{}] done working", id);
                    break;
                }
            }
        });

        match handle {
            Ok(h) => {
                return Self {
                    handle: Some(h),
                };
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                process::exit(1);
            }
        }
    }
}

/// JobPool manages a job queue to be run on a specified number of threads.
pub struct JobPool {
    size: usize,
    workers: Option<Vec<Worker>>,
    job_queue: Arc<Mutex<VecDeque<Option<BoxedJob>>>>,
    condvar: Arc<Condvar>,
}

impl JobPool {
    ///
    /// Creates a new job pool.
    ///
    /// Using the number of cpu cores as argument for size is recommended.
    /// Higher values can result in larger memory footprint,
    /// and non-optimal performance.
    ///
    /// # Panics
    ///
    /// This function will panic if the argument for size is 0.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use jobpool::JobPool;
    ///
    /// let pool_size: usize = 8; // number of cpu cores is recommended
    /// let mut pool = JobPool::new(pool_size);
    /// pool.queue(|| {
    ///     // do some work
    /// });
    /// // ...
    /// pool.shutdown(); // blocks until all jobs are done
    /// ```
    pub fn new(size: usize) -> Self {
        if size == 0 {
            panic!("size cannot be 0")
        }

        let mut workers = Vec::new();
        let job_queue = Arc::new(Mutex::new(VecDeque::new()));
        let condvar = Arc::new(Condvar::new());

        for id in 0..size {
            workers.push(Worker::new(id, job_queue.clone(), condvar.clone()));
        }

        Self {
            size,
            workers: Some(workers),
            job_queue,
            condvar,
        }
    }

    /// Queues a new "job".
    ///
    /// A queued job gets run in a first-come, first-serve basis.
    ///
    /// # Panics
    ///
    /// This method will panic if the JobPool instance has already been shutdown.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use jobpool::JobPool;
    ///
    /// let pool_size: usize = 8; // number of cpu cores is recommended
    /// let mut pool = JobPool::new(pool_size);
    /// pool.queue(|| {
    ///     // do some work
    /// });
    /// // ...
    /// pool.shutdown(); // blocks until all jobs are done
    /// ```
    pub fn queue<J>(&self, job: J)
    where
        J: Runnable + Send + 'static,
    {
        match self.workers {
            Some(_) => {
                self.push(Some(Box::new(job)));
            }
            None => {
                panic!("Error: this threadpool has been shutdown!");
            }
        }
    }

    fn push(&self, job: Option<BoxedJob>) {
        let mut guard = self.job_queue.lock().unwrap();
        guard.push_back(job);
        self.condvar.notify_one();
    }

    /// Shuts down this instance of JobPool.
    ///
    /// This method will wait for all of the queued jobs to finish.
    /// It also gets called automatically as the instance goes out of scope,
    /// so calling this method can be optional.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use jobpool::JobPool;
    ///
    /// let pool_size: usize = 8; // number of cpu cores is recommended
    /// let mut pool = JobPool::new(pool_size);
    /// pool.queue(|| {
    ///     // do some work
    /// });
    /// // ...
    /// pool.shutdown(); // blocks until all jobs are done
    /// ```
    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }

        for _ in 0..self.size {
            self.push(None);
        }

        for worker in &mut self.workers.take().unwrap() {
            if let Some(handle) = worker.handle.take() {
                // println!("[{}] shutting down", handle.thread().name().unwrap());
                handle.join().unwrap();
            }
        }
    }
}

impl Drop for JobPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use JobPool;

    #[test]
    #[allow(unused)]
    fn shuts_down() {
        let mut pool = JobPool::new(10);
        for _ in 0..100 {
            pool.queue(|| { let a = 1 + 2; });
        }
        pool.shutdown();
    }

    #[test]
    #[should_panic]
    #[allow(unused)]
    fn panic_on_reuse() {
        let mut pool = JobPool::new(10);
        for _ in 0..100 {
            pool.queue(|| { let a = 1 + 2; });
        }
        pool.shutdown();
        pool.queue(|| { let a = 1 + 2; });
    }

    #[test]
    #[allow(unused)]
    fn no_panic_on_multiple_shutdowns() {
        let mut pool = JobPool::new(10);
        for _ in 0..100 {
            pool.queue(|| { let a = 1 + 2; });
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
}
