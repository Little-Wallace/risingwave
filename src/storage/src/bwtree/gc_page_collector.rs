use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use spin::RwLock;

use crate::bwtree::delta_chain::DeltaChain;
use crate::bwtree::index_page::IndexPageHolder;
use crate::bwtree::mapping_table::PageHolder;

#[derive(Default)]
struct RemovedPagesCollector {
    pages: Vec<Arc<RwLock<DeltaChain>>>,
    index_pages: Vec<IndexPageHolder>,
}

#[derive(Default)]
struct ReadVersion {
    next: Option<ReadVersionPtr>,
    collector: RemovedPagesCollector,
}

type ReadVersionPtr = Arc<Mutex<ReadVersion>>;

pub struct ReadGuard {
    pointer: ReadVersionPtr,
}

impl ReadGuard {
    pub fn add_leaf_page(&self, page: Arc<RwLock<DeltaChain>>) {
        self.pointer.lock().collector.pages.push(page);
    }

    pub fn add_index_page(&self, page: IndexPageHolder) {
        self.pointer.lock().collector.index_pages.push(page);
    }
}

pub struct GcPageCollector {
    current_pointer: ArcSwap<Mutex<ReadVersion>>,
}

impl Default for GcPageCollector {
    fn default() -> Self {
        Self {
            current_pointer: ArcSwap::new(Arc::new(Mutex::new(ReadVersion::default()))),
        }
    }
}

impl GcPageCollector {
    pub fn get_snapshot(&self) -> ReadGuard {
        ReadGuard {
            pointer: self.current_pointer.load_full(),
        }
    }

    pub fn refresh_for_gc(&self) {
        let newest_pointer = Arc::new(Mutex::new(ReadVersion::default()));
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
