use std::ops::{Deref, DerefMut};

#[derive(Debug)]
struct DWayHeap<T: Ord, const D: usize> {
    data: Vec<T>,
}

impl<T: Ord, const D: usize> DWayHeap<T, D> {
    pub fn new() -> Self {
        DWayHeap { data: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        DWayHeap {
            data: Vec::with_capacity(cap),
        }
    }

    pub fn from_vec(vec: Vec<T>) -> Self {
        let mut h = DWayHeap { data: vec };
        if h.data.len() > 1 {
            for i in (0..=(h.data.len() - 1) / D).rev() {
                unsafe { h.bubble_down(i) }
            }
        }
        h
    }

    fn parent(i: usize) -> usize {
        if i == 0 {
            0
        } else if i % D == 0 {
            i / D - 1
        } else {
            i / D
        }
    }

    unsafe fn bubble_up(&mut self, mut i: usize) {
        let mut pi = i;
        while pi > 0 {
            i = pi;
            pi = Self::parent(i);
            if self.data.get_unchecked(pi) < self.data.get_unchecked(i) {
                self.data.swap(i, pi)
            }
        }
    }

    unsafe fn highest_priority_child(&self, i: usize) -> usize {
        let mut ret = 0;
        for cn in 1..=D {
            let ci = D * i + cn;
            if ci >= self.data.len() {
                break;
            }
            if ret == 0 || self.data.get_unchecked(ci) > self.data.get_unchecked(ret) {
                ret = ci;
            }
        }
        ret
    }

    unsafe fn bubble_down(&mut self, mut i: usize) {
        let mut ci = self.highest_priority_child(i);
        while ci > 0 {
            if self.data.get_unchecked(ci) <= self.data.get_unchecked(i) {
                break;
            }
            self.data.swap(i, ci);
            i = ci;
            ci = self.highest_priority_child(i);
        }
    }

    pub fn insert(&mut self, val: T) {
        self.data.push(val);
        unsafe { self.bubble_up(self.data.len() - 1) }
    }

    fn peek(&self) -> Option<&T> {
        self.data.get(0)
    }

    /// Similar to std::BinaryHeap::peek_mut.
    ///
    /// Note: Leaking PeekMut will cause undefined behaviour.
    fn peek_mut(&mut self) -> Option<PeekMut<T, D>> {
        if self.data.is_empty() {
            None
        } else {
            Some(PeekMut { heap: self })
        }
    }

    fn pop(&mut self) -> Option<T> {
        if self.data.len() <= 1 {
            self.data.pop()
        } else {
            let len = self.data.len();
            self.data.swap(0, len - 1);
            let ret = self.data.pop();
            unsafe {
                self.bubble_down(0);
            }
            ret
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct PeekMut<'a, T: Ord, const D: usize> {
    heap: &'a mut DWayHeap<T, D>,
}

impl<'a, T: Ord, const D: usize> Deref for PeekMut<'a, T, D> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.heap.data.get_unchecked(0) }
    }
}

impl<'a, T: Ord, const D: usize> DerefMut for PeekMut<'a, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.heap.data.get_unchecked_mut(0) }
    }
}

impl<'a, T: Ord, const D: usize> Drop for PeekMut<'a, T, D> {
    fn drop(&mut self) {
        unsafe { self.heap.bubble_down(0) }
    }
}

impl<T: Ord, const D: usize> Iterator for DWayHeap<T, D> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_heap() {
        let mut pq: DWayHeap<i64, 3> = DWayHeap::with_capacity(5);
        pq.insert(5);
        pq.insert(5);
        pq.insert(6);
        pq.insert(3);

        assert_eq!(pq.pop().unwrap(), 6);
        assert_eq!(pq.pop().unwrap(), 5);
        assert_eq!(pq.pop().unwrap(), 5);

        pq.insert(2);
        assert_eq!(*pq.peek().unwrap(), 3);
        assert_eq!(pq.pop().unwrap(), 3);

        pq.insert(7);
        assert_eq!(pq.pop().unwrap(), 7);
        assert_eq!(pq.pop().unwrap(), 2);
        assert_eq!(pq.pop(), None)
    }

    #[test]
    fn test_from_vec() {
        let mut data = vec![4, 5, 6, 3, 3, 2, 1, 3, 2, 4, 9, 10];
        let pq: DWayHeap<i32, 3> = DWayHeap::from_vec(data.clone());

        data.sort();

        assert!(data.into_iter().rev().eq(pq.into_iter()));
    }

    #[test]
    fn peek_mut() {
        let data = vec![4, 5, 6, 3, 3, 2, 1, 3, 2, 10, 4, 9];
        let mut pq: DWayHeap<i32, 3> = DWayHeap::from_vec(data);
        {
            let mut val = pq.peek_mut().unwrap();
            assert_eq!(10, *val);
            *val = 12;
        }
        assert_eq!(12, *pq.peek().unwrap());
    }

    #[test]
    fn test_seq() {
        let N = 1000;
        let mut pq: DWayHeap<usize, 2> = DWayHeap::with_capacity(N);
        for i in (1..=N).rev() {
            pq.insert(i);
        }
        // println!("{:?}", pq);
        for i in (1..=N).rev() {
            assert_eq!(i, pq.pop().unwrap());
        }
        assert_eq!(0, pq.len());
    }
}
