use rocksdb_util::FilterType;

use crate::hummock::sstable::filter::RIBBON_FILTER_MASK;
use crate::hummock::Sstable;

pub struct RibbonFilterBuilder {
    inner: rocksdb_util::FilterBitsBuilderWrapper,
    key_count: usize,
}

impl RibbonFilterBuilder {
    pub fn new(bits_per_key: u64) -> Self {
        Self {
            inner: rocksdb_util::FilterBitsBuilderWrapper::create(
                bits_per_key as f64,
                FilterType::Ribbon,
            ),
            key_count: 0,
        }
    }

    pub fn add_key(&mut self, dist_key: &[u8], table_id: u32) {
        self.key_count += 1;
        self.inner
            .add_key_hash(Sstable::hash_for_bloom_filter(dist_key, table_id));
    }

    pub fn finish(self) -> Vec<u8> {
        let mut data = self.inner.finish();
        data.push(RIBBON_FILTER_MASK);
        data
    }

    pub fn approximate_len(&self) -> usize {
        self.key_count * std::mem::size_of::<usize>()
    }
}
