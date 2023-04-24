use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use bytes::Bytes;
use risingwave_hummock_sdk::key::{FullKey, StateTableKey};

use crate::bwtree::sorted_record_block::BlockIterator;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::value::HummockValue;

pub struct MergedDataIterator<'a> {
    iters: Vec<BlockIterator<'a>>,
    heap: BinaryHeap<BlockIterator<'a>>,
}

impl<'a> MergedDataIterator<'a> {
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

    pub fn seek(&mut self, key: &[u8]) {
        for iter in self.heap.drain() {
            self.iters.push(iter);
        }
        for mut iter in self.iters.drain(..) {
            iter.seek(key);
            if iter.is_valid() {
                self.heap.push(iter);
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.heap
            .peek()
            .map(|iter| iter.is_valid())
            .unwrap_or(false)
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

    pub fn raw_value(&self) -> &[u8] {
        self.heap.peek().unwrap().raw_value()
    }

    pub fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().unwrap().value()
    }
}

pub struct Node {
    iter: SharedBufferBatchIterator<Forward>,
}
impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.iter.table_key().cmp(&self.iter.table_key())
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.iter.table_key().eq(&other.iter.table_key())
    }
}

impl Eq for Node {}

pub struct MergedSharedBufferIterator {
    iters: Vec<SharedBufferBatchIterator<Forward>>,
    heap: BinaryHeap<Node>,
}

impl MergedSharedBufferIterator {
    pub fn new(iters: Vec<SharedBufferBatchIterator<Forward>>) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(iters.len()),
            iters,
        }
    }

    pub fn seek<'a>(&'a mut self, full_key: FullKey<&'a [u8]>) {
        for node in self.heap.drain() {
            self.iters.push(node.iter);
        }
        for mut iter in self.iters.drain(..) {
            iter.sync_seek(full_key);
            if iter.is_valid() {
                self.heap.push(Node { iter });
            }
        }
    }

    pub fn seek_to_first(&mut self) {
        for node in self.heap.drain() {
            self.iters.push(node.iter);
        }
        for mut iter in self.iters.drain(..) {
            iter.sync_rewind();
            if iter.is_valid() {
                self.heap.push(Node { iter });
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.heap.is_empty()
            || self
                .heap
                .peek()
                .map(|iter| iter.iter.is_valid())
                .unwrap_or(false)
    }

    pub fn next(&mut self) {
        let mut top = self.heap.peek_mut().unwrap();
        top.iter.sync_next();
        if !top.iter.is_valid() {
            PeekMut::pop(top);
        }
    }

    pub fn key(&self) -> StateTableKey<&[u8]> {
        self.heap.peek().unwrap().iter.table_key()
    }

    pub fn current_item(&self) -> (Bytes, HummockValue<Bytes>) {
        self.heap.peek().unwrap().iter.current_item().clone()
    }

    pub fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().unwrap().iter.value()
    }
}
