use std::cmp::Ordering;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::MonitoredStreamingUploader;
use risingwave_pb::hummock::SstableInfo;

use crate::bwtree::base_page::BasePage;
use crate::bwtree::leaf_page::Delta;
use crate::bwtree::smo::IndexPageRedoLogRecord;
use crate::bwtree::PageId;
use crate::hummock::{CompressionAlgorithm, HummockResult};

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct PageInfo {
    pub pid: PageId,
    pub epoch: u64,
    pub is_page: bool,
    pub offset: usize,
    pub len: usize,
}

impl PageInfo {
    pub fn new(pid: PageId, epoch: u64, is_page: bool, offset: usize, len: usize) -> Self {
        Self {
            pid,
            epoch,
            is_page,
            offset,
            len,
        }
    }

    pub fn decode_from(buf: &mut &[u8]) -> Self {
        let pid = buf.get_u64_le();
        let epoch = buf.get_u64_le();
        let is_page = buf.get_u8() == 1;
        let offset = buf.get_u32_le() as usize;
        let len = buf.get_u32_le() as usize;
        Self::new(pid, epoch, is_page, offset, len)
    }

    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.pid);
        buf.put_u64_le(self.epoch);
        buf.put_u8(if self.is_page { 1 } else { 0 });
        buf.put_u32_le(self.offset as u32);
        buf.put_u32_le(self.len as u32);
    }
}

impl PartialOrd for PageInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.pid
                .cmp(&other.pid)
                .then_with(|| other.epoch.cmp(&self.epoch)),
        )
    }
}

impl Ord for PageInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct SegmentMeta {
    pub algorithm: CompressionAlgorithm,
    pub redo_log_offset: usize,
    pub page_offset: Vec<PageInfo>,
    pub index_page_offset: Vec<PageInfo>,
}

impl SegmentMeta {
    pub fn get_page_offset(&self, pid: PageId) -> Option<(usize, usize)> {
        let mut pos = self.page_offset.partition_point(|item| item.pid < pid);
        while pos < self.page_offset.len() && self.page_offset[pos].pid == pid {
            if self.page_offset[pos].is_page {
                return Some((self.page_offset[pos].offset, self.page_offset[pos].len));
            }
            pos += 1;
        }
        None
    }

    pub fn get_delta_offset(&self, pid: PageId) -> Vec<(usize, usize)> {
        let mut ret = vec![];
        let mut pos = self.page_offset.partition_point(|item| item.pid < pid);
        while pos < self.page_offset.len() && self.page_offset[pos].pid == pid {
            if !self.page_offset[pos].is_page {
                ret.push((self.page_offset[pos].offset, self.page_offset[pos].len));
            }
            pos += 1;
        }
        ret
    }

    pub fn decode_from(buf: &mut &[u8]) -> HummockResult<Self> {
        let algorithm = CompressionAlgorithm::decode(buf)?;
        let redo_log_offset = buf.get_u64_le() as usize;
        let page_offset_len = buf.get_u32_le() as usize;
        let mut page_offset = Vec::with_capacity(page_offset_len);
        for _ in 0..page_offset_len {
            page_offset.push(PageInfo::decode_from(buf));
        }
        let index_page_offset_len = buf.get_u32_le() as usize;
        let mut index_page_offset = Vec::with_capacity(index_page_offset_len);
        for _ in 0..index_page_offset_len {
            index_page_offset.push(PageInfo::decode_from(buf));
        }
        Ok(Self {
            algorithm,
            redo_log_offset,
            page_offset,
            index_page_offset,
        })
    }

    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.algorithm.into());
        buf.put_u32_le(self.redo_log_offset as u32);
        buf.put_u32_le(self.page_offset.len() as u32);
        for offset in &self.page_offset {
            offset.encode_into(buf);
        }
        buf.put_u32_le(self.index_page_offset.len() as u32);
        for offset in &self.index_page_offset {
            offset.encode_into(buf);
        }
    }
}

pub struct SegmentBuilder {
    id: HummockSstableObjectId,
    algorithm: CompressionAlgorithm,
    writer: MonitoredStreamingUploader,
    data_offset: Vec<PageInfo>,
    index_page_offset: Vec<PageInfo>,
    redo_log_offset: usize,
    data_len: usize,
    max_epoch: u64,
    min_epoch: u64,
}

impl SegmentBuilder {
    pub fn open(
        id: HummockSstableObjectId,
        algorithm: CompressionAlgorithm,
        writer: MonitoredStreamingUploader,
    ) -> Self {
        Self {
            id,
            algorithm,
            writer,
            data_offset: vec![],
            index_page_offset: vec![],
            redo_log_offset: 0,
            data_len: 0,
            max_epoch: u64::MIN,
            min_epoch: u64::MAX,
        }
    }

    pub async fn append_page(&mut self, page: Arc<BasePage>) -> HummockResult<()> {
        self.min_epoch = std::cmp::min(self.min_epoch, page.epoch());
        self.max_epoch = std::cmp::max(self.max_epoch, page.epoch());
        let data = page.compress(self.algorithm);
        let mut buf = BytesMut::with_capacity(page.encode_size() + std::mem::size_of::<u32>());
        page.encode_meta(&mut buf);
        buf.put_u32_le(data.len() as u32);
        self.data_offset.push(PageInfo::new(
            page.get_page_id(),
            page.epoch(),
            true,
            self.data_len,
            buf.len() + data.len(),
        ));
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_delta(&mut self, paeg_id: PageId, delta: Arc<Delta>) -> HummockResult<()> {
        self.max_epoch = std::cmp::max(self.max_epoch, delta.max_epoch());
        let data = delta.data();
        let meta_size = std::mem::size_of::<PageId>() + std::mem::size_of::<u64>() * 2;
        let mut buf = BytesMut::with_capacity(meta_size + std::mem::size_of::<u32>());
        buf.put_u64_le(paeg_id);
        buf.put_u64_le(delta.max_epoch());
        buf.put_u64_le(delta.prev_epoch());
        self.data_offset.push(PageInfo::new(
            paeg_id,
            delta.max_epoch(),
            false,
            buf.len() + data.len(),
            self.data_len,
        ));
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_index_page(
        &mut self,
        page_id: PageId,
        epoch: u64,
        data: Bytes,
    ) -> HummockResult<()> {
        self.max_epoch = std::cmp::max(self.max_epoch, epoch);
        self.index_page_offset.push(PageInfo::new(
            page_id,
            epoch,
            true,
            self.data_len,
            data.len(),
        ));
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_redo_log(
        &mut self,
        logs: Vec<IndexPageRedoLogRecord>,
    ) -> HummockResult<()> {
        self.redo_log_offset = self.data_len;
        for log in &logs {
            log.deltas.iter().for_each(|delta| {
                self.min_epoch = std::cmp::min(self.min_epoch, delta.epoch);
                self.max_epoch = std::cmp::max(self.max_epoch, delta.epoch);
            });
        }
        let mut buf = BytesMut::new();
        buf.put_u32_le(logs.len() as u32);
        for log in logs {
            log.encode_into(&mut buf);
        }
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        Ok(())
    }

    pub async fn finish(mut self) -> HummockResult<SstableInfo> {
        let meta_offset = self.data_len as u64;
        self.data_offset.sort();
        self.index_page_offset.sort();
        let meta = SegmentMeta {
            algorithm: self.algorithm,
            page_offset: self.data_offset,
            index_page_offset: self.index_page_offset,
            redo_log_offset: self.redo_log_offset,
        };
        let mut buf = BytesMut::new();
        meta.encode_into(&mut buf);
        let meta_len = buf.len() as u64;
        self.writer.write_bytes(buf.freeze()).await?;
        self.writer.finish().await?;
        Ok(SstableInfo {
            object_id: self.id,
            sst_id: self.id,
            key_range: None,
            file_size: meta_offset + meta_len,
            table_ids: vec![],
            meta_offset,
            stale_key_count: 0,
            total_key_count: 0,
            min_epoch: self.min_epoch,
            max_epoch: self.max_epoch,
            uncompressed_file_size: 0,
        })
    }
}

pub struct SegmentIterator {}
