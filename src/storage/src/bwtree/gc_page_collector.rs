use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::PageId;

#[derive(Default)]
struct RemovedPagesCollector {
    pages: Vec<PageId>,
    index_pages: Vec<PageId>,
}

struct ReadVersion {
    next: Option<ReadVersionPtr>,
    collector: RemovedPagesCollector,
    mapping_table: Arc<MappingTable>,
}

impl ReadVersion {
    pub fn new(mapping_table: Arc<MappingTable>) -> Self {
        Self {
            collector: RemovedPagesCollector::default(),
            next: None,
            mapping_table,
        }
    }
}

impl Drop for ReadVersion {
    fn drop(&mut self) {
        for p in &self.collector.pages {
            self.mapping_table.remove_delta_chains(p);
        }
        for p in &self.collector.index_pages {
            self.mapping_table.remove_index_delta(p);
        }
    }
}

type ReadVersionPtr = Arc<Mutex<ReadVersion>>;

pub struct ReadGuard {
    pointer: ReadVersionPtr,
}

impl ReadGuard {
    pub fn add_leaf_page(&self, pages: &[PageId]) {
        self.pointer.lock().collector.pages.extend(pages);
    }

    pub fn add_index_page(&self, pages: &[PageId]) {
        self.pointer.lock().collector.index_pages.extend(pages);
    }
}

pub struct GcPageCollector {
    current_pointer: ArcSwap<Mutex<ReadVersion>>,
    mapping_table: Arc<MappingTable>,
}

impl GcPageCollector {
    pub fn new(mapping_table: Arc<MappingTable>) -> Self {
        Self {
            current_pointer: ArcSwap::new(Arc::new(Mutex::new(ReadVersion::new(
                mapping_table.clone(),
            )))),
            mapping_table,
        }
    }

    pub fn get_snapshot(&self) -> ReadGuard {
        ReadGuard {
            pointer: self.current_pointer.load_full(),
        }
    }

    pub fn refresh_for_gc(&self) {
        let newest_pointer = Arc::new(Mutex::new(ReadVersion::new(self.mapping_table.clone())));
        let old = self.current_pointer.load();
        let mut old_ptr = old.clone();
        let mut prev = self
            .current_pointer
            .compare_and_swap(old, newest_pointer.clone());
        while !std::ptr::eq(prev.as_ref(), old_ptr.as_ref()) {
            old_ptr = prev.clone();
            prev = self
                .current_pointer
                .compare_and_swap(prev, newest_pointer.clone());
        }
        old_ptr.lock().next = Some(newest_pointer);
    }
}
