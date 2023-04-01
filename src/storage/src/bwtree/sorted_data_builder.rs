use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::KeyComparator;

use super::sorted_record_block::KeyPrefix;
use crate::hummock::sstable::utils::{xxhash64_checksum, CompressionAlgorithm};
use crate::hummock::HummockError;

pub const SPLIT_LEAF_CAPACITY: usize = 50 * 1024;
pub const DEFAULT_RESTART_INTERVAL: usize = 16;
pub const DEFAULT_ENTRY_SIZE: usize = 24; // table_id(u64) + primary_key(u64) + epoch(u64)

pub struct BlockBuilderOptions {
    /// Reserved bytes size when creating buffer to avoid frequent allocating.
    pub capacity: usize,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    /// Restart point interval.
    pub restart_interval: usize,
}

impl Default for BlockBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: SPLIT_LEAF_CAPACITY,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        }
    }
}

/// [`BlockBuilder`] encodes and appends block to a buffer.
pub struct BlockBuilder {
    /// Write buffer.
    buf: BytesMut,
    /// Entry interval between restart points.
    restart_count: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    /// Last key.
    last_key: Vec<u8>,
    /// Count of entries in current block.
    entry_count: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorithm,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new(BlockBuilderOptions::default())
    }
}

impl BlockBuilder {
    pub fn new(options: BlockBuilderOptions) -> Self {
        Self {
            // add more space to avoid re-allocate space.
            buf: BytesMut::with_capacity(options.capacity + 256),
            restart_count: options.restart_interval,
            restart_points: Vec::with_capacity(
                options.capacity / DEFAULT_ENTRY_SIZE / options.restart_interval + 1,
            ),
            last_key: vec![],
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
        }
    }

    /// Appends a kv pair to the block.
    ///
    /// NOTE: Key must be added in ASCEND order.
    ///
    /// # Format
    ///
    /// ```plain
    /// entry (kv pair): | overlap len (2B) | diff len (2B) | value len(4B) | diff key | value |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if key is not added in ASCEND order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.entry_count > 0 {
            debug_assert!(!key.is_empty());
            debug_assert_eq!(
                KeyComparator::compare_encoded_full_key(&self.last_key[..], key),
                std::cmp::Ordering::Less
            );
        }
        // Update restart point if needed and calculate diff key.
        let diff_key = if self.entry_count % self.restart_count == 0 {
            self.restart_points.push(self.buf.len() as u32);
            key
        } else {
            super::sorted_record_block::bytes_diff(&self.last_key, key)
        };

        let prefix = KeyPrefix {
            overlap: key.len() - diff_key.len(),
            diff: diff_key.len(),
            value: value.len(),
            offset: self.buf.len(),
        };

        prefix.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        self.buf.put_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    pub fn get_last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.restart_points.clear();
        self.last_key.clear();
        self.entry_count = 0;
    }

    /// Calculate block size without compression.
    pub fn uncompressed_block_size(&mut self) -> usize {
        self.buf.len() + (self.restart_points.len() + 1) * std::mem::size_of::<u32>()
    }

    /// Finishes building block.
    ///
    /// # Format
    ///
    /// ```plain
    /// compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) |
    /// uncompressed: | compression method (1B) | crc32sum (4B) |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if there is compression error.
    pub fn build(mut self) -> Bytes {
        assert!(self.entry_count > 0);
        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }
        self.buf.put_u32_le(self.restart_points.len() as u32);
        self.buf.put_u32_le(self.entry_count as u32);
        match self.compression_algorithm {
            CompressionAlgorithm::None => (),
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(BytesMut::with_capacity(self.buf.len()).writer())
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let (writer, result) = encoder.finish();
                result.map_err(HummockError::encode_error).unwrap();
                self.buf = writer.into_inner();
            }
            CompressionAlgorithm::Zstd => {
                let mut encoder =
                    zstd::Encoder::new(BytesMut::with_capacity(self.buf.len()).writer(), 4)
                        .map_err(HummockError::encode_error)
                        .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let writer = encoder
                    .finish()
                    .map_err(HummockError::encode_error)
                    .unwrap();
                self.buf = writer.into_inner();
            }
        };
        self.compression_algorithm.encode(&mut self.buf);
        let checksum = xxhash64_checksum(&self.buf);
        self.buf.put_u64_le(checksum);
        self.buf.freeze()
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        // block + restart_points + restart_points.len + compression_algorithm + checksum
        self.buf.len() + 4 * self.restart_points.len() + 4 + 1 + 8
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use risingwave_hummock_sdk::key::{user_key, StateTableKey, TableKey};

    use crate::bwtree::sorted_data_builder::BlockBuilder;
    use crate::bwtree::sorted_record_block::SortedRecordBlock;
    use crate::hummock::value::HummockValue;

    #[test]
    fn test_data_build() {
        let mut builder = BlockBuilder::default();
        let data = [
            StateTableKey::new(TableKey(b"aa".to_vec()), 1),
            StateTableKey::new(TableKey(b"bb".to_vec()), 1),
            StateTableKey::new(TableKey(b"cc".to_vec()), 1),
        ];
        for k in &data {
            let mut raw_key = BytesMut::new();
            let mut raw_value = BytesMut::new();
            let v = HummockValue::Put(k.user_key.0.clone());
            v.encode(&mut raw_value);
            k.encode_into(&mut raw_key);
            builder.add(&raw_key, &raw_value);
            raw_key.clear();
            raw_value.clear();
        }
        let ret = builder.build();
        let block = SortedRecordBlock::decode(ret, 0).unwrap();
        let mut iter = block.iter();
        iter.seek_to_first();
        let mut idx = 0;
        while iter.is_valid() {
            assert_eq!(data[idx].user_key.as_ref(), user_key(iter.key()));
            assert_eq!(
                data[idx].user_key.as_ref(),
                iter.value().into_user_value().unwrap()
            );
            iter.next();
            idx += 1;
        }
    }
}
