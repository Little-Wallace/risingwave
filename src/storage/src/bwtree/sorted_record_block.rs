// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::io::{Read, Write};
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::KeyComparator;
use zstd::zstd_safe::WriteBuf;
use {lz4, zstd};

use crate::hummock::sstable::utils::{xxhash64_checksum, xxhash64_verify, CompressionAlgorithm};
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockError, HummockResult};

#[derive(Clone)]
pub struct SortedRecordBlock {
    /// Uncompressed entries data, with restart encoded restart points info.
    data: Bytes,
    /// Uncompressed entried data length.
    data_len: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    record_count: usize,
}

impl SortedRecordBlock {
    pub fn decode(buf: Bytes, uncompressed_capacity: usize) -> HummockResult<Self> {
        // Verify checksum.
        let xxhash64_checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], xxhash64_checksum)?;

        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let compressed_data = &buf[..buf.len() - 9];
        let buf = match compression {
            CompressionAlgorithm::None => buf.slice(0..(buf.len() - 9)),
            CompressionAlgorithm::Lz4 => {
                let mut decoder = lz4::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                debug_assert_eq!(decoded.capacity(), uncompressed_capacity);
                Bytes::from(decoded)
            }
            CompressionAlgorithm::Zstd => {
                let mut decoder = zstd::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                debug_assert_eq!(decoded.capacity(), uncompressed_capacity);
                Bytes::from(decoded)
            }
        };

        Ok(Self::decode_from_raw(buf))
    }

    pub fn decode_from_raw(data: Bytes) -> Self {
        // Decode restart points.
        let raw_data_len = data.len() - 8;
        let mut buf = &data.as_slice()[raw_data_len..];
        let n_restarts = buf.get_u32_le();
        let record_count = buf.get_u32_le() as usize;
        let data_len = raw_data_len - n_restarts as usize * 4;
        let mut restart_points = Vec::with_capacity(n_restarts as usize);
        let mut restart_points_buf = &buf[data_len..raw_data_len];
        for _ in 0..n_restarts {
            restart_points.push(restart_points_buf.get_u32_le());
        }

        SortedRecordBlock {
            data,
            data_len,
            restart_points,
            record_count,
        }
    }

    /// Entries data len.
    #[expect(clippy::len_without_is_empty)]
    pub fn size(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data_len
    }

    pub fn capacity(&self) -> usize {
        self.data.len() + self.restart_points.capacity() * std::mem::size_of::<u32>()
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Gets restart point by index.
    pub fn restart_point(&self, index: usize) -> u32 {
        self.restart_points[index]
    }

    /// Gets restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Searches the index of the restart point that the given `offset` belongs to.
    pub fn search_restart_point(&self, offset: usize) -> usize {
        // Find the largest restart point that equals or less than the given offset.
        self.restart_points
            .partition_point(|&position| position <= offset as u32)
            .saturating_sub(1) // Prevent from underflowing when given is smaller than the first.
    }

    /// Searches the index of the restart point by partition point.
    pub fn search_restart_partition_point<P>(&self, pred: P) -> usize
    where
        P: FnMut(&u32) -> bool,
    {
        self.restart_points.partition_point(pred)
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..self.data_len]
    }

    pub fn raw_data(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn iter<'a>(&'a self) -> BlockIterator<'a> {
        BlockIterator::new(self)
    }

    pub fn get_middle_key(&self) -> Bytes {
        let restart_index = self.restart_points.len() / 2;
        if restart_index == 0 {
            let mut iter = self.iter();
            iter.seek_to_first();
            let mut count = 0;
            while count < self.record_count / 2 {
                iter.next();
            }
            iter.key.freeze()
        } else {
            let offset = self.restart_point(restart_index) as usize;
            let prefix = KeyPrefix::decode(&mut &self.data()[offset..], offset);
            Bytes::copy_from_slice(&self.data()[prefix.diff_key_range()])
        }
    }
}

/// [`KeyPrefix`] contains info for prefix compression.
#[derive(Debug)]
pub struct KeyPrefix {
    pub overlap: usize,
    pub diff: usize,
    pub value: usize,
    /// Used for calculating range, won't be encoded.
    pub offset: usize,
}

impl KeyPrefix {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.overlap as u16);
        buf.put_u16(self.diff as u16);
        buf.put_u32(self.value as u32);
    }

    pub fn decode(buf: &mut impl Buf, offset: usize) -> Self {
        let overlap = buf.get_u16() as usize;
        let diff = buf.get_u16() as usize;
        let value = buf.get_u32() as usize;
        Self {
            overlap,
            diff,
            value,
            offset,
        }
    }

    /// Encoded length.
    fn len(&self) -> usize {
        2 + 2 + 4
    }

    /// Gets overlap len.
    pub fn overlap_len(&self) -> usize {
        self.overlap
    }

    /// Gets diff key range.
    pub fn diff_key_range(&self) -> Range<usize> {
        self.offset + self.len()..self.offset + self.len() + self.diff
    }

    /// Gets value range.
    pub fn value_range(&self) -> Range<usize> {
        self.offset + self.len() + self.diff..self.offset + self.len() + self.diff + self.value
    }

    /// Gets entry len.
    pub fn entry_len(&self) -> usize {
        self.len() + self.diff + self.value
    }
}

/// [`BlockIterator`] is used to read kv pairs in a block.
pub struct BlockIterator<'a> {
    /// Block that iterates on.
    block: &'a SortedRecordBlock,
    /// Current restart point index.
    restart_point_index: usize,
    /// Current offset.
    offset: usize,
    /// Current key.
    key: BytesMut,
    /// Current value.
    value_range: Range<usize>,
    /// Current entry len.
    entry_len: usize,
}

impl<'a> BlockIterator<'a> {
    pub fn new(block: &'a SortedRecordBlock) -> Self {
        Self {
            block,
            offset: usize::MAX,
            restart_point_index: usize::MAX,
            key: BytesMut::default(),
            value_range: 0..0,
            entry_len: 0,
        }
    }

    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.next_inner();
    }

    pub fn try_next(&mut self) -> bool {
        assert!(self.is_valid());
        self.try_next_inner()
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());
        self.prev_inner();
    }

    pub fn try_prev(&mut self) -> bool {
        assert!(self.is_valid());
        self.try_prev_inner()
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key[..]
    }

    pub fn raw_value(&self) -> &[u8] {
        &self.block.data()[self.value_range.clone()]
    }

    pub fn value(&self) -> HummockValue<&[u8]> {
        assert!(self.is_valid());
        HummockValue::from_slice(self.raw_value()).expect("decode error")
    }

    pub fn is_valid(&self) -> bool {
        self.offset < self.block.size()
    }

    pub fn seek_to_first(&mut self) {
        self.seek_restart_point_by_index(0);
    }

    pub fn seek_to_last(&mut self) {
        self.seek_restart_point_by_index(self.block.restart_point_len() - 1);
        self.next_until_prev_offset(self.block.size());
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
    }

    pub fn seek_le(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
        if !self.is_valid() {
            self.seek_to_last();
        }
        self.prev_until_key(key);
    }
}

impl<'a> BlockIterator<'a> {
    /// Invalidates current state after reaching a invalid state.
    fn invalidate(&mut self) {
        self.offset = self.block.size();
        self.restart_point_index = self.block.restart_point_len();
        self.key.clear();
        self.value_range = 0..0;
        self.entry_len = 0;
    }

    /// Moving to the next entry
    ///
    /// Note: The current state may be invalid if there is no more data to read
    fn next_inner(&mut self) {
        if !self.try_next_inner() {
            self.invalidate();
        }
    }

    /// Try moving to the next entry.
    ///
    /// The current state will still be valid if there is no more data to read.
    ///
    /// Return: true is the iterator is advanced and false otherwise.
    fn try_next_inner(&mut self) -> bool {
        let offset = self.offset + self.entry_len;
        if offset >= self.block.size() {
            return false;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        if self.restart_point_index + 1 < self.block.restart_point_len()
            && self.offset >= self.block.restart_point(self.restart_point_index + 1) as usize
        {
            self.restart_point_index += 1;
        }
        true
    }

    /// Moves forward until reaching the first that equals or larger than the given `key`.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && KeyComparator::compare_encoded_full_key(&self.key[..], key) == Ordering::Less
        {
            self.next_inner();
        }
    }

    /// Moves backward until reaching the first key that equals or smaller than the given `key`.
    fn prev_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && KeyComparator::compare_encoded_full_key(&self.key[..], key) == Ordering::Greater
        {
            self.prev_inner();
        }
    }

    /// Moves forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_prev_offset(&mut self, offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.size(), offset) {
            self.next_inner();
        }
    }

    /// Moving to the previous entry
    ///
    /// Note: The current state may be invalid if there is no more data to read
    fn prev_inner(&mut self) {
        if !self.try_prev_inner() {
            self.invalidate();
        }
    }

    /// Try moving to the previous entry.
    ///
    /// The current state will still be valid if there is no more data to read.
    ///
    /// Return: true is the iterator is advanced and false otherwise.
    fn try_prev_inner(&mut self) -> bool {
        if self.offset == 0 {
            return false;
        }
        if self.block.restart_point(self.restart_point_index) as usize == self.offset {
            self.restart_point_index -= 1;
        }
        let origin_offset = self.offset;
        self.seek_restart_point_by_index(self.restart_point_index);
        self.next_until_prev_offset(origin_offset);
        true
    }

    /// Decodes [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.block.data()[offset..], offset)
    }

    /// Searches the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        // Find the largest restart point that restart key equals or less than the given key.
        self.block
            .search_restart_partition_point(|&probe| {
                let prefix = self.decode_prefix_at(probe as usize);
                let probe_key = &self.block.data()[prefix.diff_key_range()];
                match KeyComparator::compare_encoded_full_key(probe_key, key) {
                    Ordering::Less | Ordering::Equal => true,
                    Ordering::Greater => false,
                }
            })
            .saturating_sub(1) // Prevent from underflowing when given is smaller than the first.
    }

    /// Seeks to the restart point that the given `key` belongs to.
    fn seek_restart_point_by_key(&mut self, key: &[u8]) {
        let index = self.search_restart_point_index_by_key(key);
        self.seek_restart_point_by_index(index)
    }

    /// Seeks to the restart point by given restart point index.
    fn seek_restart_point_by_index(&mut self, index: usize) {
        let offset = self.block.restart_point(index) as usize;
        let prefix = self.decode_prefix_at(offset);
        self.key = BytesMut::from(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        self.restart_point_index = index;
    }
}

impl<'a> PartialOrd for BlockIterator<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for BlockIterator<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        KeyComparator::compare_encoded_full_key(other.key(), &self.key())
    }
}

impl<'a> PartialEq for BlockIterator<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key().eq(other.key())
    }
}

impl<'a> Eq for BlockIterator<'a> {}

unsafe fn to_u64(ptr: *const u8) -> u64 {
    std::ptr::read_unaligned(ptr as *const u64)
}

unsafe fn to_u32(ptr: *const u8) -> u32 {
    std::ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff<'a>(base: &[u8], target: &'a [u8]) -> &'a [u8] {
    let end = std::cmp::min(base.len(), target.len());
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if to_u64(base.as_ptr().add(i)) != to_u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end && to_u32(base.as_ptr().add(i)) == to_u32(target.as_ptr().add(i)) {
            i += 4;
        }
        while i < end {
            if base.get_unchecked(i) != target.get_unchecked(i) {
                return target.get_unchecked(i..);
            }
            i += 1;
        }
        target.get_unchecked(end..)
    }
}
