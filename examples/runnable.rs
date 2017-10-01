extern crate jobpool;

use jobpool::{JobPool, Runnable};
use std::thread;
use std::time::Duration;

struct SomeStruct {
    some_val: usize,
}

impl Runnable for SomeStruct {
    fn run(self: Box<Self>) {
        thread::sleep(Duration::from_millis(self.some_val as u64));
        println!("some_val: {}", self.some_val);
    }
}

fn main() {
    let pool_size = 8; // or number of cpu cores
    let mut pool = JobPool::new(pool_size);

    for i in 0..100 {
        pool.queue(SomeStruct {
            some_val: i,
        });
    }

    pool.shutdown();
    println!("JobPool has shutdown!");
}
