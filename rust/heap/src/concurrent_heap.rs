use crate::concurrent_heap::Item::{Available, Empty, InProgress};
use crossbeam_utils::CachePadded;
use parking_lot::{Condvar, Mutex};
use std::fmt::Pointer;
use std::thread::ThreadId;

// Algorithm reference: https://www.cs.rochester.edu/u/scott/papers/1996_IPL_heaps.pdf

#[derive(Debug)]
enum Item<T> {
    Empty,
    Available(T),
    InProgress(T, ThreadId),
}

impl<T> Item<T> {
    fn make_available(&mut self) {
        if let InProgress(v, _) = std::mem::replace(self, Empty) {
            *self = Available(v)
        } else {
            unreachable!();
        }
    }

    fn take_val(&mut self) -> T {
        if let Available(v) | InProgress(v, _) = std::mem::replace(self, Empty) {
            v
        } else {
            unreachable!()
        }
    }

    fn get_val(&self) -> Option<&T> {
        match self {
            Available(v) | InProgress(v, _) => Some(v),
            Empty => None,
        }
    }
}

struct ScopeCall<F: FnMut()> {
    c: F,
}

impl<F: FnMut()> Drop for ScopeCall<F> {
    fn drop(&mut self) {
        (self.c)();
    }
}

#[derive(Debug)]
struct ConcurrentHeap<T: Ord> {
    cap: usize,
    // todo: a ticket based spin lock should be more suitable here: https://crates.io/crates/spin
    data: Box<[CachePadded<Mutex<Item<T>>>]>,
    size: Mutex<usize>,
    not_full: Condvar,
    not_empty: Condvar,
}

impl<T: Ord> ConcurrentHeap<T> {
    pub fn new(cap: usize) -> Self {
        if cap == 0 {
            panic!("0 cap ConcurrentHeap requested")
        }
        let mut data = Vec::with_capacity(cap);
        for _ in 0..cap {
            data.push(CachePadded::new(Mutex::new(Empty)))
        }
        let data = data.into_boxed_slice();
        ConcurrentHeap {
            data,
            cap,
            size: Mutex::new(0),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    fn parent(i: usize) -> usize {
        if i == 0 {
            0
        } else if i % 2 == 0 {
            i / 2 - 1
        } else {
            i / 2
        }
    }

    pub fn push(&self, val: T) {
        // note: unlike pop, we notify waiting threads only after the item is fully pushed and Available
        let _defer = ScopeCall {
            c: || _ = self.not_empty.notify_one(),
        };

        let my_id = std::thread::current().id();
        let mut pos: usize;
        {
            let mut size_guard = self.size.lock();
            while *size_guard == self.cap {
                self.not_full.wait(&mut size_guard);
            }

            pos = *size_guard;
            let mut slot = self.data[pos].lock();
            *size_guard += 1;
            drop(size_guard);

            assert!(matches!(*slot, Empty));
            if pos == 0 {
                *slot = Available(val);
                return;
            }
            *slot = InProgress(val, my_id);
        }

        // sift up (pos > 0)
        loop {
            let parent_pos = Self::parent(pos);
            let mut parent_slot = self.data[parent_pos].lock();
            let mut my_slot = self.data[pos].lock();
            match (&*my_slot, &*parent_slot) {
                (_, Empty) => return, // parent empty, some concurrent delete fixed our inserted item
                (InProgress(v, tid), Available(pv)) if *tid == my_id => {
                    if v > pv {
                        std::mem::swap(&mut *my_slot, &mut *parent_slot);
                        pos = parent_pos;
                    } else {
                        my_slot.make_available();
                        return;
                    }
                }
                (InProgress(_, tid), InProgress(..)) if *tid == my_id => continue, // spin wait for parent to become available
                (_, _) => pos = parent_pos,                                        // look up
            }
            if pos > 0 {
                continue;
            }
            if let InProgress(_, tid) = &*parent_slot {
                if *tid == my_id {
                    parent_slot.make_available();
                }
            }
            return;
        }
    }

    pub fn pop(&self) -> T {
        let mut curr_slot;
        let popped_val;
        {
            // note: unlike push, we notify not_full as soon as space is available.
            let _defer = ScopeCall {
                c: || _ = self.not_full.notify_one(),
            };

            let mut size_guard = self.size.lock();
            while *size_guard == 0 {
                self.not_empty.wait(&mut size_guard);
            }

            *size_guard -= 1;
            let bottom = *size_guard;
            curr_slot = self.data[0].lock();
            let mut bottom_slot = None;
            if bottom > 0 {
                bottom_slot = Some(self.data[bottom].lock());
            }
            drop(size_guard);

            popped_val = curr_slot.take_val(); // also asserts top slot is available
            if let Some(mut bottom_slot) = bottom_slot {
                std::mem::swap(&mut *curr_slot, &mut *bottom_slot);
            } else {
                return popped_val;
            }
        }

        // sift down
        let mut curr_pos = 0;
        'sift_down: while 2 * curr_pos + 1 < self.cap {
            let left = 2 * curr_pos + 1;
            let right = left + 1;

            let mut child = self.data[left].lock();
            let mut ci = left;

            if right < self.cap {
                let right_slot = self.data[right].lock();
                let lv = child.get_val();
                let rv = right_slot.get_val();
                match (lv, rv) {
                    (None, _) => break 'sift_down,
                    (Some(lv), Some(rv)) if rv > lv => {
                        child = right_slot;
                        ci = right
                    }
                    (_, _) => {}
                }
            }

            match child.get_val() {
                Some(cv) if cv > curr_slot.get_val().unwrap() => {
                    std::mem::swap(&mut *curr_slot, &mut *child);
                    curr_pos = ci;
                    curr_slot = child;
                }
                _ => break 'sift_down,
            }
        }

        popped_val
    }

    fn len(&self) -> usize {
        *self.size.lock()
    }
}
#[cfg(test)]
mod tests {
    use crate::concurrent_heap::{ConcurrentHeap, Item};
    use std::sync::{Arc, Mutex};
    #[test]
    fn test_heap() {
        let mut pq: ConcurrentHeap<i64> = ConcurrentHeap::new(10);
        pq.push(5);
        pq.push(5);
        pq.push(6);
        pq.push(3);

        assert_eq!(pq.pop(), 6);
        assert_eq!(pq.pop(), 5);
        assert_eq!(pq.pop(), 5);

        pq.push(2);
        assert_eq!(pq.pop(), 3);

        pq.push(7);
        assert_eq!(pq.pop(), 7);
        assert_eq!(pq.pop(), 2);
    }

    #[test]
    fn test_seq() {
        let N = 1000;
        let pq = Arc::new(ConcurrentHeap::<usize>::new(N));
        for i in (1..=N).rev() {
            pq.push(i);
        }
        for i in 0..N {
            assert!(matches!(*pq.data[i].lock(), Item::Available(_)));
            assert_eq!(N - i, *pq.data[i].lock().get_val().unwrap());
        }
        // println!("{:?}", pq);
        for n in (1..=N).rev() {
            assert_eq!(n, pq.pop());
            for j in 0..n - 1 {
                assert!(matches!(*pq.data[j].lock(), Item::Available(_)));
            }
        }
        assert_eq!(0, pq.len());
    }

    #[test]
    fn test_concurrent_seq() {
        let N = 15;
        let C = N;
        let R = 1000000;

        for run in 1..R {
            println!("run={}", run);
            let pq = Arc::new(ConcurrentHeap::<usize>::new(C));

            let pusher = || {
                for i in (1..=N).rev() {
                    pq.push(i);
                }
            };

            let popper = || {
                for n in (1..=N).rev() {
                    assert_eq!(n, pq.pop());
                }
            };
            std::thread::scope(|s| {
                s.spawn(pusher);
                s.spawn(popper);
            });
        }
        #[test]
        fn test_concurrent_seq_less_cap() {
            let N = 1000;
            let C = N / 2;
            let R = 100000;

            for run in 1..R {
                println!("run={}", run);
                let pq = Arc::new(ConcurrentHeap::<usize>::new(C));

                let pusher = || {
                    for i in (1..=N).rev() {
                        pq.push(i);
                    }
                };

                let popper = || {
                    for n in (1..=N).rev() {
                        assert_eq!(n, pq.pop());
                    }
                };
                std::thread::scope(|s| {
                    s.spawn(pusher);
                    s.spawn(popper);
                });
            }
        }
    }

    #[repr(align(64))]
    struct TestA {
        vals: [u8; 100],
    }
    #[test]
    fn test_align() {
        println!("{}", size_of::<TestA>());
    }
}
