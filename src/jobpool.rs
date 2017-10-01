#![warn(missing_docs)]

use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::{process, thread};
use std::collections::{HashMap, VecDeque};

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
    fn new(
        id: usize,
        id_counter: Arc<AtomicUsize>,
        job_queue: Arc<Mutex<VecDeque<BoxedJob>>>,
        condvar: Arc<Condvar>,
        workers: Arc<Mutex<HashMap<usize, Worker>>>,
        removed_handles: Arc<Mutex<Vec<Option<thread::JoinHandle<()>>>>>,
        busy_workers_count: Arc<AtomicUsize>,
        min_size: Arc<usize>,
        max_size: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let builder = thread::Builder::new().name(format!("worker-{}", id));
        let handle = builder.spawn(move || loop {
            let (job, remaining_job_count) = {
                let mut guard = job_queue.lock().unwrap();
                while guard.is_empty() && !shutdown.load(Ordering::SeqCst) {
                    // println!("[worker-{}] waiting...", id);
                    guard = condvar.wait(guard).unwrap();
                    // println!("[worker-{}] notified", id);
                }

                // println!("[worker-{}] got new new job", id);

                // queue is not empty at this point, so unwrap() is safe
                (guard.pop_front(), guard.len())
            };

            if job.is_none() {
                break;
            }

            let job = job.unwrap();

            busy_workers_count.fetch_add(1, Ordering::SeqCst);

            let max_size_prime = max_size.load(Ordering::SeqCst);
            if max_size_prime != 0 {
                let mut guard = workers.lock().unwrap();
                // println!("remaining job count: {}", remaining_job_count);
                if remaining_job_count > guard.len() {
                    let busy_workers = busy_workers_count.load(Ordering::SeqCst);
                    if guard.len() < max_size_prime && busy_workers >= *min_size && !shutdown.load(Ordering::SeqCst) {
                        let new_id = id_counter.fetch_add(1, Ordering::SeqCst);
                        // println!("inserting new one: {}", new_id);
                        guard.insert(
                            new_id,
                            Worker::new(
                                new_id,
                                id_counter.clone(),
                                job_queue.clone(),
                                condvar.clone(),
                                workers.clone(),
                                removed_handles.clone(),
                                busy_workers_count.clone(),
                                min_size.clone(),
                                max_size.clone(),
                                shutdown.clone(),
                            ),
                        );
                        // println!("new workers size: {}", guard.len());
                    }
                }
                // drop(guard); // commented out just for the reminder...
            }

            // println!("[worker-{}] running job", id);
            job.run();

            busy_workers_count.fetch_sub(1, Ordering::SeqCst);

            let mut guard = workers.lock().unwrap();
            if guard.len() > *min_size && busy_workers_count.load(Ordering::SeqCst) < *min_size {
                let worker = guard.remove(&id);
                drop(guard);

                if let Some(worker) = worker {
                    if let Some(handle) = worker.handle {
                        let mut guard = removed_handles.lock().unwrap();
                        guard.push(Some(handle));
                    }
                }

                // println!("[worker-{}] done working and REMOVED", id);
                break;
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
    size: Arc<usize>,
    max_size: Arc<AtomicUsize>,
    busy_workers_count: Arc<AtomicUsize>,
    workers: Arc<Mutex<HashMap<usize, Worker>>>,
    removed_handles: Arc<Mutex<Vec<Option<thread::JoinHandle<()>>>>>,
    job_queue: Arc<Mutex<VecDeque<BoxedJob>>>,
    condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
}

impl JobPool {
    ///
    /// Creates a new job pool.
    ///
    /// Using the number of cpu cores as the argument for `size` is recommended.
    /// Higher values can result iSeqCstn larger memory footprint,
    /// and non-optimal performance.
    ///
    /// # Panics
    ///
    /// This function will panic if the argument for `size` is 0.
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

        let job_queue = Arc::new(Mutex::new(VecDeque::new()));
        let condvar = Arc::new(Condvar::new());
        let max_size = Arc::new(AtomicUsize::new(0));
        let worker_id_counter = Arc::new(AtomicUsize::new(size));
        let busy_workers_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let size = Arc::new(size);
        let removed_handles = Arc::new(Mutex::new(Vec::new()));

        let workers = {
            let workers = Arc::new(Mutex::new(HashMap::new()));
            let mut guard = workers.lock().unwrap();
            for id in 0..*size {
                guard.insert(
                    id,
                    Worker::new(
                        id,
                        worker_id_counter.clone(),
                        job_queue.clone(),
                        condvar.clone(),
                        workers.clone(),
                        removed_handles.clone(),
                        busy_workers_count.clone(),
                        size.clone(),
                        max_size.clone(),
                        shutdown.clone(),
                    ),
                );
            }
            workers.clone()
        };

        Self {
            size,
            max_size,
            busy_workers_count,
            workers,
            removed_handles,
            job_queue,
            condvar,
            shutdown,
        }
    }

    /// Queues a new "job".
    ///
    /// A "job" can be a closure with no arguments and returns, or
    /// a type with `Runnable` trait. A queued job gets run in a first-come, first-serve basis.
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
    pub fn queue<J>(&mut self, job: J)
    where
        J: Runnable + Send + 'static,
    {
        if self.shutdown.load(Ordering::SeqCst) {
            panic!("Error: this threadpool has been shutdown!");
        } else {
            self.push(Box::new(job));
        }
    }

    /// Automatically increase the number of worker threads as needed until `max_size` is reached.
    ///
    /// # Panics
    ///
    /// This method will panic if `max_size` is less than or equal to initial JobPool size.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use jobpool::JobPool;
    ///
    /// let pool_size: usize = 8; // number of cpu cores is recommended
    /// let mut pool = JobPool::new(pool_size);
    /// pool.auto_grow(100);
    /// for _ in 0..1000 {
    ///     pool.queue(|| {
    ///         // do some work
    ///     });
    /// }
    /// // ...
    /// pool.shutdown(); // blocks until all jobs are done
    /// ```
    pub fn auto_grow(&mut self, max_size: usize) {
        if max_size <= *self.size {
            panic!("max_size must be greater than initial JobPool size");
        }
        self.max_size.store(max_size, Ordering::SeqCst);
    }

    /// Get the number of current active worker threads.
    pub fn active_workers_count(&self) -> usize {
        return self.busy_workers_count.load(Ordering::SeqCst);
    }

    fn push(&mut self, job: BoxedJob) {
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
        if !self.can_shutdown() {
            return;
        }

        let mut handles = Vec::new();

        let mut guard = self.workers.lock().unwrap();
        handles.reserve(guard.len());

        for (_, worker) in &mut *guard {
            if let Some(handle) = worker.handle.take() {
                // println!("[{}] shutting down", handle.thread().name().unwrap());
                handles.push(handle);
            }
        }
        drop(guard);

        let mut guard = self.removed_handles.lock().unwrap();
        for handle in &mut *guard {
            handles.push(handle.take().unwrap());
        }
        drop(guard);

        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Shuts down this instance of JobPool without waiting for threads to finish.
    ///
    /// This method will return all of the threads' `JoinHandle`s (as few as initial JobPool size,
    /// and as many as additional *active* threads if `auto_grow(...)` has been used.
    /// It won't wait for the threads to finish, and it must be called explicitly.
    /// Once this method is called, this instance of JobPool will throw away jobs already in the queue while
    /// running the one at hand.
    /// Calling `shutdown()` after this method won't have any effect.
    /// Unlike `shutdown()`, it doesn't get called automatically after going out of scope.
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
    /// let handles = pool.shutdown_no_wait();
    /// // ...
    /// if let Some(handles) = handles {
    ///     for handle in handles {
    ///         let _ = handle.join();
    ///     }
    /// }
    /// ```
    pub fn shutdown_no_wait(&mut self) -> Option<Vec<thread::JoinHandle<()>>> {
        if !self.can_shutdown() {
            return None;
        }

        let mut handles = Vec::new();

        let mut guard = self.workers.lock().unwrap();
        handles.reserve(guard.len());

        for (_, worker) in &mut *guard {
            if let Some(handle) = worker.handle.take() {
                // println!("[{}] shutting down", handle.thread().name().unwrap());
                handles.push(handle);
            }
        }
        drop(guard);

        let mut guard = self.removed_handles.lock().unwrap();
        for handle in &mut *guard {
            handles.push(handle.take().unwrap());
        }
        drop(guard);

        Some(handles)
    }

    fn can_shutdown(&mut self) -> bool {
        if self.shutdown.load(Ordering::SeqCst) {
            return false;
        }

        self.shutdown.store(true, Ordering::SeqCst);
        self.condvar.notify_all();

        return true;
    }
}

impl Drop for JobPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}