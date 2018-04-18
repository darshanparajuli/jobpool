#![warn(missing_docs)]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::{process, thread};

type BoxedRunnable = Box<Runnable + Send + 'static>;

/// A constant reference value for normal priority.
pub const NORMAL_PRIORITY: isize = 0;

/// A trait for giving a type an ability to run some code.
pub trait Runnable {
    /// Runs some code.
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Runnable for F {
    #[inline(always)]
    fn run(self: Box<Self>) {
        self()
    }
}

struct Job {
    runnable: BoxedRunnable,
    priority: isize,
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Job) -> Option<Ordering> {
        Some(self.priority.cmp(&other.priority))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Job) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Job) -> bool {
        self.priority == other.priority
    }
}

impl Eq for Job {
}

fn spawn_worker_thread(
    id: usize,
    id_counter: Arc<AtomicUsize>,
    job_queue: Arc<Mutex<BinaryHeap<Job>>>,
    condvar: Arc<Condvar>,
    workers: Arc<Mutex<HashMap<usize, Option<thread::JoinHandle<()>>>>>,
    removed_handles: Arc<Mutex<Vec<Option<thread::JoinHandle<()>>>>>,
    busy_workers_count: Arc<AtomicUsize>,
    min_size: Arc<usize>,
    max_size: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
) -> Option<thread::JoinHandle<()>> {
    let builder = thread::Builder::new().name(format!("worker-{}", id));
    let handle = builder.spawn(move || loop {
        let (job, remaining_job_count) = {
            let mut guard = job_queue.lock().unwrap();
            while guard.is_empty() && !shutdown.load(AtomicOrdering::SeqCst) {
                // println!("[worker-{}] waiting...", id);
                guard = condvar.wait(guard).unwrap();
                // println!("[worker-{}] notified", id);
            }

            // println!("[worker-{}] got new new job", id);

            // queue is not empty at this point, so unwrap() is safe
            (guard.pop(), guard.len())
        };

        if job.is_none() {
            break;
        }

        let job = job.unwrap();

        busy_workers_count.fetch_add(1, AtomicOrdering::SeqCst);

        let max_size_val = try_add_new_worker(
            id_counter.clone(),
            job_queue.clone(),
            condvar.clone(),
            workers.clone(),
            removed_handles.clone(),
            busy_workers_count.clone(),
            min_size.clone(),
            max_size.clone(),
            shutdown.clone(),
            Some(remaining_job_count),
        );

        // println!("[worker-{}] running job with priority {}", id, job.priority);
        job.runnable.run();

        busy_workers_count.fetch_sub(1, AtomicOrdering::SeqCst);

        if max_size_val > 0 {
            let mut guard = workers.lock().unwrap();
            if guard.len() > *min_size && busy_workers_count.load(AtomicOrdering::SeqCst) < *min_size {
                let worker = guard.remove(&id);
                drop(guard);

                if let Some(worker) = worker {
                    if let Some(handle) = worker {
                        let mut guard = removed_handles.lock().unwrap();
                        guard.push(Some(handle));
                    }
                }

                // println!("[worker-{}] done working and REMOVED", id);
                break;
            }
        }
    });

    match handle {
        Ok(h) => Some(h),
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}

fn try_add_new_worker(
    id_counter: Arc<AtomicUsize>,
    job_queue: Arc<Mutex<BinaryHeap<Job>>>,
    condvar: Arc<Condvar>,
    workers: Arc<Mutex<HashMap<usize, Option<thread::JoinHandle<()>>>>>,
    removed_handles: Arc<Mutex<Vec<Option<thread::JoinHandle<()>>>>>,
    busy_workers_count: Arc<AtomicUsize>,
    min_size: Arc<usize>,
    max_size: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    remaining_job_count: Option<usize>,
) -> usize {
    let max_size_prime = max_size.load(AtomicOrdering::SeqCst);
    if max_size_prime > 0 {
        // println!("remaining job count: {}", remaining_job_count);
        let busy_workers = busy_workers_count.load(AtomicOrdering::SeqCst);
        let mut guard = workers.lock().unwrap();
        let current_workers = guard.len();
        let available_workers = if busy_workers > current_workers {
            0
        } else {
            current_workers - busy_workers
        };

        if let Some(remaining_job_count) = remaining_job_count {
            if remaining_job_count <= available_workers {
                return max_size_prime;
            }
        }

        if guard.len() < max_size_prime && busy_workers >= *min_size && !shutdown.load(AtomicOrdering::SeqCst) {
            let new_id = id_counter.fetch_add(1, AtomicOrdering::SeqCst);
            // println!("inserting new one: {}", new_id);
            guard.insert(
                new_id,
                spawn_worker_thread(
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
        // drop(guard); // commented out just for the reminder...
    }

    max_size_prime
}

/// JobPool manages a job queue to be run on a specified number of threads.
pub struct JobPool {
    size: Arc<usize>,
    max_size: Arc<AtomicUsize>,
    worker_id_counter: Arc<AtomicUsize>,
    busy_workers_count: Arc<AtomicUsize>,
    workers: Arc<Mutex<HashMap<usize, Option<thread::JoinHandle<()>>>>>,
    removed_handles: Arc<Mutex<Vec<Option<thread::JoinHandle<()>>>>>,
    job_queue: Arc<Mutex<BinaryHeap<Job>>>,
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

        let job_queue = Arc::new(Mutex::new(BinaryHeap::new()));
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
                    spawn_worker_thread(
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
            worker_id_counter,
            busy_workers_count,
            workers,
            removed_handles,
            job_queue,
            condvar,
            shutdown,
        }
    }

    /// Queues a new `job`.
    ///
    /// A `job` can be a closure with no arguments and returns, or
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
        self.queue_job(job, NORMAL_PRIORITY);
    }

    /// Queues a new `job` with a given `priority`.
    ///
    /// A `job` can be a closure with no arguments and returns, or
    /// a type with `Runnable` trait. A queued job with highest priority runs at given time.
    /// The value for `priority` is totally relative. The constant `NORMAL_PRIORITY` (value = 0) can be used as
    /// reference.
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
    ///
    /// for i in -10..10 {
    ///     pool.queue_with_priority(move || {
    ///         // do some work
    ///     }, jobpool::NORMAL_PRIORITY + i);
    /// }
    /// // ...
    /// pool.shutdown(); // blocks until all jobs are done
    /// ```
    pub fn queue_with_priority<J>(&mut self, job: J, priority: isize)
    where
        J: Runnable + Send + 'static,
    {
        self.queue_job(job, priority);
    }

    fn queue_job<J>(&mut self, job: J, priority: isize)
    where
        J: Runnable + Send + 'static,
    {
        if self.shutdown.load(AtomicOrdering::SeqCst) {
            panic!("Error: this threadpool has been shutdown!");
        } else {
            let mut guard = self.job_queue.lock().unwrap();
            guard.push(Job {
                runnable: Box::new(job),
                priority: priority,
            });
            self.condvar.notify_one();
            drop(guard);

            try_add_new_worker(
                self.worker_id_counter.clone(),
                self.job_queue.clone(),
                self.condvar.clone(),
                self.workers.clone(),
                self.removed_handles.clone(),
                self.busy_workers_count.clone(),
                self.size.clone(),
                self.max_size.clone(),
                self.shutdown.clone(),
                None,
            );
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
        self.max_size.store(max_size, AtomicOrdering::SeqCst);
    }

    /// Get the number of current active worker threads.
    pub fn active_workers_count(&self) -> usize {
        self.busy_workers_count.load(AtomicOrdering::SeqCst)
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
        if self.has_shutdown() {
            return;
        }

        self.notify_shutdown();

        let mut handles = Vec::new();

        let mut guard = self.workers.lock().unwrap();
        handles.reserve(guard.len());

        for (_, worker) in &mut *guard {
            if let Some(handle) = worker.take() {
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
        if self.has_shutdown() {
            return None;
        }

        self.notify_shutdown();

        let mut handles = Vec::new();

        let mut guard = self.workers.lock().unwrap();
        handles.reserve(guard.len());

        for (_, worker) in &mut *guard {
            if let Some(handle) = worker.take() {
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

    /// Check whether this JobPool instance has been shutdown.
    pub fn has_shutdown(&self) -> bool {
        self.shutdown.load(AtomicOrdering::SeqCst)
    }

    fn notify_shutdown(&mut self) {
        self.shutdown.store(true, AtomicOrdering::SeqCst);
        self.condvar.notify_all();
    }
}

impl Drop for JobPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
