# JobPool

A simple and lightweight thread pool implementation for Rust.

[![Build Status](https://travis-ci.org/darshanparajuli/jobpool.svg?branch=master)](https://travis-ci.org/darshanparajuli/jobpool)

## Example
```rust
extern crate jobpool;

use jobpool::JobPool;
use std::thread;
use std::time::Duration;
use std::sync::mpsc;

fn main() {
    let pool_size = 8; // or number of cpu cores
    let mut pool = JobPool::new(pool_size);

    let (tx, rx) = mpsc::channel();

    for i in 0..100 {
        let tx = tx.clone();
        pool.queue(move || {
            // Do some work, following is just an example
            thread::sleep(Duration::from_millis(100));
            println!("sending: {}", i);
            tx.send(i).unwrap();
        });
    }

    for _ in 0..100 {
        match rx.recv() {
            Ok(val) => println!("received: {}", val),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    // Explicit call to shutdown; JobPool shuts down automatically after
    // going out of scope as well.
    pool.shutdown();
}
```
