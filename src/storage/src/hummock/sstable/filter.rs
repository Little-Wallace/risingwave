use rocksdb_util::FilterBitsReaderWrapper;

use crate::hummock::sstable::bloom::{BloomFilterBuilder, BloomFilterReader};
use crate::hummock::sstable::ribbon_filter::RibbonFilterBuilder;

pub enum FilterReader {
    Bloom(BloomFilterReader),
    RocksDB(FilterBitsReaderWrapper),
    Empty,
}

pub const RIBBON_FILTER_MASK: u8 = 127;

impl FilterReader {
    pub fn new(mut data: Vec<u8>) -> Self {
        if data.is_empty() {
            return FilterReader::Empty;
        }
        let num_probes = data.last().unwrap();
        if *num_probes == RIBBON_FILTER_MASK {
            data.pop();
            FilterReader::RocksDB(FilterBitsReaderWrapper::create(data))
        } else {
            FilterReader::Bloom(BloomFilterReader::new(data))
        }
    }

    #[inline(always)]
    pub fn may_match_hash(&self, hash: u64) -> bool {
        match self {
            FilterReader::RocksDB(reader) => reader.hash_may_match(hash),
            FilterReader::Bloom(reader) => reader.may_match(hash as u32),
            FilterReader::Empty => true,
        }
    }
}

pub enum FilterBuilder {
    Bloom(BloomFilterBuilder),
    Ribbon(RibbonFilterBuilder),
}

impl FilterBuilder {
    /// add key which need to be filter for construct filter data.
    pub fn add_key(&mut self, dist_key: &[u8], table_id: u32) {
        match self {
            FilterBuilder::Bloom(builder) => builder.add_key(dist_key, table_id),
            FilterBuilder::Ribbon(builder) => builder.add_key(dist_key, table_id),
        }
    }

    /// Builds Bloom filter from key hashes
    pub fn finish(self) -> Vec<u8> {
        match self {
            FilterBuilder::Bloom(builder) => builder.finish(),
            FilterBuilder::Ribbon(builder) => builder.finish(),
        }
    }

    pub fn approximate_len(&self) -> usize {
        match self {
            FilterBuilder::Bloom(builder) => builder.approximate_len(),
            FilterBuilder::Ribbon(builder) => builder.approximate_len(),
        }
    }
}
