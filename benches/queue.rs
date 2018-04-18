#![feature(test)]

extern crate jobpool;
extern crate test;

use test::Bencher;
use jobpool::JobPool;

#[bench]
fn queue(b: &mut Bencher) {
    let mut pool = JobPool::new(8);
    b.iter(|| {
        pool.queue(|| {
            let some_calculation = 1 + 2;
            let _ = some_calculation;
        });
    });
}
