#![warn(missing_docs)]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
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

impl Eq for Job {}

struct WorkerState {
    workers: HashMap<usize, Worker>,
    removed_handles: Vec<Option<thread::JoinHandle<()>>>,
    busy_workers: usize,
    id_counter: usize,
}

struct Worker {
    id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        id: usize,
        worker_state: Arc<Mutex<WorkerState>>,
        job_queue: Arc<Mutex<BinaryHeap<Job>>>,
        condvar: Arc<Condvar>,
        min_size: Arc<usize>,
        max_size: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let builder = thread::Builder::new().name(format!("worker-{}", id));
        let handle = builder.spawn(move || loop {
            let job = {
                let mut guard = job_queue.lock().unwrap();
                while guard.is_empty() && !shutdown.load(AtomicOrdering::SeqCst) {
                    // println!("[worker-{}] waiting...", id);
                    guard = condvar.wait(guard).unwrap();
                    // println!("[worker-{}] notified", id);
                }

                // println!("[worker-{}] got new new job", id);

                // queue is not empty at this point, so unwrap() is safe
                guard.pop()
            };

            if job.is_none() {
                break;
            }

            let job = job.unwrap();

            {
                let mut guard = worker_state.lock().unwrap();
                if guard.busy_workers < guard.workers.len() {
                    guard.busy_workers += 1;
                }
            }

            let auto_grow = max_size.load(AtomicOrdering::SeqCst) > *min_size;
            if auto_grow {
                JobPool::try_grow(
                    worker_state.clone(),
                    job_queue.clone(),
                    condvar.clone(),
                    min_size.clone(),
                    max_size.clone(),
                    shutdown.clone(),
                );
            }

            // println!("[worker-{}] running job with priority {}", id, job.priority);
            job.runnable.run();

            let mut guard = worker_state.lock().unwrap();
            if guard.busy_workers > 0 {
                guard.busy_workers -= 1;
            }

            if auto_grow {
                if guard.workers.len() > *min_size && guard.busy_workers < *min_size {
                    let worker = guard.workers.remove(&id);
                    if let Some(worker) = worker {
                        if let Some(handle) = worker.handle {
                            guard.removed_handles.push(Some(handle));
                        }
                    }

                    // println!("[worker-{}] done working and REMOVED", id);
                    break;
                }
            }
        });

        let handle = match handle {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("Error: {}", e);
                process::exit(1);
            }
        };

        Self { id, handle }
    }
}

/// JobPool manages a job queue to be run on a specified number of threads.
pub struct JobPool {
    size: Arc<usize>,
    max_size: Arc<AtomicUsize>,
    worker_state: Arc<Mutex<WorkerState>>,
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
        let max_size = Arc::new(AtomicUsize::new(size));
        let shutdown = Arc::new(AtomicBool::new(false));
        let size = Arc::new(size);

        let worker_state = Arc::new(Mutex::new(WorkerState {
            workers: HashMap::new(),
            removed_handles: Vec::new(),
            busy_workers: 0,
            id_counter: 0,
        }));

        {
            let mut guard = worker_state.lock().unwrap();
            for id in 0..*size {
                guard.workers.insert(
                    id,
                    Worker::new(
                        id,
                        worker_state.clone(),
                        job_queue.clone(),
                        condvar.clone(),
                        size.clone(),
                        max_size.clone(),
                        shutdown.clone(),
                    ),
                );
            }
        }

        Self {
            size,
            max_size,
            worker_state,
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
            self.push_new_job(job, priority);
            self.condvar.notify_one();

            if self.max_size.load(AtomicOrdering::SeqCst) > *self.size {
                Self::try_grow(
                    self.worker_state.clone(),
                    self.job_queue.clone(),
                    self.condvar.clone(),
                    self.size.clone(),
                    self.max_size.clone(),
                    self.shutdown.clone()
                );
            }
        }
    }

    fn try_grow(
        worker_state: Arc<Mutex<WorkerState>>,
        job_queue: Arc<Mutex<BinaryHeap<Job>>>,
        condvar: Arc<Condvar>,
        min_size: Arc<usize>,
        max_size: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
    ) {
        // println!("remaining job count: {}", remaining_job_count);

        let remaining_job_count = {
            let guard = job_queue.lock().unwrap();
            guard.len()
        };

        let mut guard = worker_state.lock().unwrap();
        let busy_workers = guard.busy_workers;
        let total_workers = guard.workers.len();
        let max_size_val = max_size.load(AtomicOrdering::SeqCst);

        assert!(total_workers <= max_size_val);
        assert!(busy_workers <= total_workers);

        if busy_workers < total_workers || total_workers == max_size_val {
            return;
        }

        let available_workers = total_workers - busy_workers;
        if remaining_job_count <= available_workers {
            return;
        }

        if shutdown.load(AtomicOrdering::SeqCst) {
            return;
        }

        let new_id = {
            guard.id_counter += 1;
            guard.id_counter
        };
        // println!("inserting new one: {}", new_id);
        guard.workers.insert(
            new_id,
            Worker::new(
                new_id,
                worker_state.clone(),
                job_queue.clone(),
                condvar.clone(),
                min_size.clone(),
                max_size.clone(),
                shutdown.clone(),
            ),
        );
        // println!("new workers size: {}", guard.len());
        // drop(guard); // commented out just for the reminder...
    }

    fn push_new_job<J>(&mut self, job: J, priority: isize)
    where
        J: Runnable + Send + 'static,
    {
        let mut guard = self.job_queue.lock().unwrap();
        guard.push(Job {
            runnable: Box::new(job),
            priority: priority,
        });
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
        let guard = self.worker_state.lock().unwrap();
        guard.busy_workers
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

        let handles = {
            let mut handles = Vec::new();
            let mut guard = self.worker_state.lock().unwrap();
            handles.reserve(guard.workers.len() + guard.removed_handles.len());

            for (_, worker) in &mut guard.workers {
                if let Some(handle) = worker.handle.take() {
                    // println!("[{}] shutting down", handle.thread().name().unwrap());
                    handles.push((Some(worker.id), handle));
                }
            }

            for handle in &mut guard.removed_handles {
                handles.push((None, handle.take().unwrap()));
            }
            handles
        };

        for (id, handle) in handles {
            match handle.join() {
                Ok(_) => (),
                Err(e) => match id {
                    Some(id) => eprintln!("Error joining worker-{} thread: {:?}", id, e),
                    None => eprintln!("Error joining thread: {:?}", e),
                },
            }
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

        let mut guard = self.worker_state.lock().unwrap();
        handles.reserve(guard.workers.len() + guard.removed_handles.len());

        for (_, worker) in &mut guard.workers {
            if let Some(handle) = worker.handle.take() {
                // println!("[{}] shutting down", handle.thread().name().unwrap());
                handles.push(handle);
            }
        }

        for handle in &mut guard.removed_handles {
            handles.push(handle.take().unwrap());
        }

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
