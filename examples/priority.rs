extern crate jobpool;

use jobpool::JobPool;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() {
    let pool_size = 8; // or number of cpu cores
    let mut pool = JobPool::new(pool_size);

    let (tx, rx) = mpsc::channel();

    for i in 0..100 {
        let tx = tx.clone();
        pool.queue_with_priority(
            move || {
                thread::sleep(Duration::from_millis(100));
                println!("sending: {}", i);
                tx.send(i).unwrap();
            },
            i,
        );
    }

    for _ in 0..100 {
        match rx.recv() {
            Ok(val) => println!("received: {}", val),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    pool.shutdown();
    println!("JobPool has shutdown!");
}
