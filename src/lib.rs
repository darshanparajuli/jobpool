use std::sync::{Arc, Condvar, Mutex};
use std::{process, thread};
use std::collections::VecDeque;

type BoxedJob = Box<Runnable + Send + 'static>;

pub trait Runnable {
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Runnable for F {
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
            let work: Option<BoxedJob>;

            let mut guard = work_queue.lock().unwrap();
            while guard.is_empty() {
                // println!("[worker-{}] waiting...", id);
                guard = condvar.wait(guard).unwrap();
                // println!("[worker-{}] notified", id);
            }

            match guard.pop_front() {
                Some(front) => {
                    // println!("[worker-{}] new job", id);
                    work = front;
                    drop(guard);
                }
                None => unreachable!("Uh-oh! This shouldn't be happening...!!!"),
            }

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

pub struct JobPool {
    size: usize,
    workers: Option<Vec<Worker>>,
    job_queue: Arc<Mutex<VecDeque<Option<BoxedJob>>>>,
    condvar: Arc<Condvar>,
}

impl JobPool {
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

    pub fn queue<J>(&mut self, job: J)
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
}
