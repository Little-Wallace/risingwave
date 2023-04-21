use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::MonitoredStreamingUploader;
use risingwave_pb::hummock::SstableInfo;

use crate::bwtree::delta_chain::Delta;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::smo::IndexPageRedoLogRecord;
use crate::bwtree::PageId;
use crate::hummock::{CompressionAlgorithm, HummockResult};

pub struct SegmentMeta {
    algorithm: CompressionAlgorithm,
    redo_log_offset: usize,
    page_offset: Vec<(PageId, u64, usize)>,
    delta_offset: Vec<(PageId, u64, usize)>,
    index_page_offset: Vec<(PageId, u64, usize)>,
}

impl SegmentMeta {
    pub fn decode_from(buf: &mut &[u8]) -> HummockResult<Self> {
        let algorithm = CompressionAlgorithm::decode(buf)?;
        let redo_log_offset = buf.get_u64_le() as usize;
        let page_offset_len = buf.get_u32_le() as usize;
        let mut page_offset = Vec::with_capacity(page_offset_len);
        for _ in 0..page_offset_len {
            let pid = buf.get_u64_le();
            let epoch = buf.get_u64_le();
            let offset = buf.get_u64_le() as usize;
            page_offset.push((pid, epoch, offset));
        }
        let delta_offset_len = buf.get_u32_le() as usize;
        let mut delta_offset = Vec::with_capacity(delta_offset_len);
        for _ in 0..delta_offset_len {
            let pid = buf.get_u64_le();
            let epoch = buf.get_u64_le();
            let offset = buf.get_u64_le() as usize;
            delta_offset.push((pid, epoch, offset));
        }
        let index_page_offset_len = buf.get_u32_le() as usize;
        let mut index_page_offset = Vec::with_capacity(index_page_offset_len);
        for _ in 0..index_page_offset_len {
            let pid = buf.get_u64_le();
            let epoch = buf.get_u64_le();
            let offset = buf.get_u64_le() as usize;
            index_page_offset.push((pid, epoch, offset));
        }
        Ok(Self {
            algorithm,
            redo_log_offset,
            page_offset,
            delta_offset,
            index_page_offset,
        })
    }

    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.algorithm.into());
        buf.put_u32_le(self.redo_log_offset as u32);
        buf.put_u32_le(self.page_offset.len() as u32);
        for (pid, epoch, offset) in &self.page_offset {
            buf.put_u64_le(*pid);
            buf.put_u64_le(*epoch);
            buf.put_u64_le(*offset as u64);
        }
        buf.put_u32_le(self.delta_offset.len() as u32);
        for (pid, epoch, offset) in &self.delta_offset {
            buf.put_u64_le(*pid);
            buf.put_u64_le(*epoch);
            buf.put_u64_le(*offset as u64);
        }
        buf.put_u32_le(self.index_page_offset.len() as u32);
        for (pid, epoch, offset) in &self.index_page_offset {
            buf.put_u64_le(*pid);
            buf.put_u64_le(*epoch);
            buf.put_u64_le(*offset as u64);
        }
    }
}

pub struct SegmentBuilder {
    id: HummockSstableObjectId,
    algorithm: CompressionAlgorithm,
    writer: MonitoredStreamingUploader,
    page_offset: Vec<(PageId, u64, usize)>,
    delta_offset: Vec<(PageId, u64, usize)>,
    index_page_offset: Vec<(PageId, u64, usize)>,
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
            page_offset: vec![],
            delta_offset: vec![],
            index_page_offset: vec![],
            redo_log_offset: 0,
            data_len: 0,
            max_epoch: u64::MIN,
            min_epoch: u64::MAX,
        }
    }

    pub async fn append_page(&mut self, page: Arc<LeafPage>) -> HummockResult<()> {
        self.page_offset
            .push((page.get_page_id(), page.epoch(), self.data_len));
        self.min_epoch = std::cmp::min(self.min_epoch, page.epoch());
        self.max_epoch = std::cmp::max(self.max_epoch, page.epoch());
        let data = page.compress(self.algorithm);
        let mut buf = BytesMut::with_capacity(page.encode_size() + std::mem::size_of::<u32>() * 2);
        let total_size = page.encode_size() + data.len() + std::mem::size_of::<u32>() /* size of data.len() */;
        buf.put_u32_le(total_size as u32);
        buf.put_u64_le(page.get_page_id());
        buf.put_u64_le(page.get_right_link());
        buf.put_u64_le(page.epoch());
        buf.put_u32_le(page.smallest_user_key.len() as u32);
        buf.put_slice(page.smallest_user_key.as_ref());
        buf.put_u32_le(page.largest_user_key.len() as u32);
        buf.put_slice(page.largest_user_key.as_ref());
        buf.put_u32_le(data.len() as u32);
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_delta(&mut self, paeg_id: PageId, delta: Arc<Delta>) -> HummockResult<()> {
        self.max_epoch = std::cmp::max(self.max_epoch, delta.max_epoch());
        self.delta_offset
            .push((paeg_id, delta.max_epoch(), self.data_len));
        let data = delta.data();
        let meta_size = std::mem::size_of::<PageId>() + std::mem::size_of::<u64>() * 2;
        let mut buf = BytesMut::with_capacity(meta_size + std::mem::size_of::<u32>());
        buf.put_u32_le((meta_size + data.len()) as u32);
        buf.put_u64_le(paeg_id);
        buf.put_u64_le(delta.max_epoch());
        buf.put_u64_le(delta.prev_epoch());
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
        self.index_page_offset.push((page_id, epoch, self.data_len));
        let total_size = (std::mem::size_of::<PageId>() + data.len()) as u32;
        let mut buf =
            BytesMut::with_capacity(std::mem::size_of::<PageId>() + std::mem::size_of::<u32>());
        buf.put_u32_le(total_size);
        buf.put_u64_le(page_id);
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
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
        let meta = SegmentMeta {
            algorithm: self.algorithm,
            page_offset: self.page_offset,
            delta_offset: self.delta_offset,
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
