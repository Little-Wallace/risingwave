use rocksdb_util::FilterType;
use crate::hummock::Sstable;
use crate::hummock::sstable::bloom::FilterBuilder;

pub struct RibbonFilterBuilder {
    inner: rocksdb_util::FilterBitsBuilderWrapper,
    key_count: usize,
}

impl RibbonFilterBuilder {
    pub fn new(bits_per_key: u64) -> Self {
        Self {
            inner: rocksdb_util::FilterBitsBuilderWrapper::create(bits_per_key as f64, FilterType::Ribbon),
            key_count: 0,
        }
    }
}

impl FilterBuilder for RibbonFilterBuilder {
    fn add_key(&mut self, dist_key: &[u8], table_id: u32) {
        self.key_count += 1;
        self.inner.add_key_hash(Sstable::hash_for_bloom_filter(dist_key, table_id));
    }

    fn finish(&mut self) -> Vec<u8> {
        self.inner.finish()
    }

    fn approximate_len(&self) -> usize {
        self.key_count * std::mem::size_of::<usize>()
    }
}