use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::bwtree::sorted_record_block::BlockIterator;

pub struct DataIterator<'a> {
    iters: Vec<BlockIterator<'a>>,
    heap: BinaryHeap<BlockIterator<'a>>,
}

impl<'a> DataIterator<'a> {
    pub fn new(iters: Vec<BlockIterator<'a>>) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(iters.len()),
            iters,
        }
    }
    
    pub fn seek_to_first(&mut self) {
        for iter in self.heap.drain() {
            self.iters.push(iter);
        }
        for mut iter in self.iters.drain(..) {
            iter.seek_to_first();
            if iter.is_valid() {
                self.heap.push(iter);
            }
        }
    }
    
    pub fn is_valid(&self) -> bool {
        self.heap.peek().map(|iter|iter.is_valid()).unwrap_or(false)
    }
    
    pub fn next(&mut self) {
        let mut top = self.heap.peek_mut().unwrap();
        top.next();
        if !top.is_valid() {
            PeekMut::pop(top);
        }
    }
    
    pub fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().value()
    }
}
