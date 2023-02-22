//  Copyright 2023 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task::TaskType;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::CompactionConfig;

use super::{create_compaction_task, LevelCompactionPicker, TierCompactionPicker};
use crate::hummock::compaction::level_selector::{DynamicLevelSelectorCore, SelectContext};
use crate::hummock::compaction::{
    create_overlap_strategy, CompactionPicker, CompactionTask, DynamicLevelSelector, LevelSelector,
    LocalPickerStatistic, LocalSelectorStatistic, MinOverlappingPicker,
};
use crate::hummock::level_handler::LevelHandler;
use crate::hummock::model::CompactionGroup;

pub struct SplitTableLevelSelector {}

const SCORE_BASE: u64 = 100;

#[derive(Default)]
struct TableContext {
    pub level_max_bytes: Vec<u64>,
    pub level_bytes: Vec<u64>,
    pub base_level: usize,
    pub max_level_size: u64,
}

impl TableContext {
    pub fn new(max_level: usize) -> Self {
        Self {
            level_max_bytes: vec![0; max_level + 1],
            level_bytes: vec![0; max_level + 1],
            base_level: 0,
            max_level_size: 0,
        }
    }

    pub fn set_level_bytes(&mut self, level_idx: usize, last_table_bytes: u64) {
        if self.base_level == 0 {
            self.base_level = level_idx;
        }
        self.level_bytes[level_idx] = last_table_bytes;
        self.max_level_size = std::cmp::max(self.max_level_size, last_table_bytes);
    }
}

#[derive(Default)]
struct TableLevel {
    table_id: u32,
    select_level: usize,
    target_level: usize,
    score: u64,
}

#[derive(Default)]
struct SelectTableContext {
    pub inner: SelectContext,
    pub table_infos: HashMap<u32, TableContext>,
    pub score_levels: Vec<TableLevel>,
}

impl SelectTableContext {
    pub fn set_level_bytes(
        &mut self,
        table_id: u32,
        max_level: usize,
        level_idx: usize,
        last_table_bytes: u64,
    ) {
        if last_table_bytes > 0 {
            let table_ctx = self
                .table_infos
                .entry(table_id)
                .or_insert_with(|| TableContext::new(max_level));
            table_ctx.set_level_bytes(level_idx, last_table_bytes);
        }
    }
}

impl SplitTableLevelSelector {
    fn calculate_level_scores(
        &self,
        config: &Arc<CompactionConfig>,
        levels: &Levels,
        handlers: &mut [LevelHandler],
    ) -> Option<SelectTableContext> {
        let mut ctx = SelectTableContext::default();
        for level in &levels.levels {
            let mut last_table_id = 0;
            let mut last_table_bytes = 0;
            for sst in &level.table_infos {
                if sst.table_ids.len() > 1 {
                    return None;
                }
                if sst.table_ids[0] != last_table_id {
                    ctx.set_level_bytes(
                        last_table_id,
                        config.max_level as usize,
                        level.level_idx as usize,
                        last_table_bytes,
                    );
                    last_table_id = sst.table_ids[0];
                    last_table_bytes = sst.file_size;
                } else {
                    last_table_bytes += sst.file_size;
                }
            }
            ctx.set_level_bytes(
                last_table_id,
                config.max_level as usize,
                level.level_idx as usize,
                last_table_bytes,
            );
        }
        let core = DynamicLevelSelectorCore::new(config.clone());
        ctx.inner = core.calculate_level_base_size(levels);
        for table_id in levels.member_table_ids.clone() {
            let mut table_ctx = ctx
                .table_infos
                .entry(table_id)
                .or_insert_with(|| TableContext::new(config.max_level as usize));
            if table_ctx.max_level_size == 0 {
                // Use the bottommost level.
                table_ctx.base_level = config.max_level as usize;
                continue;
            }

            let base_bytes_max = config.max_bytes_for_level_base;
            let base_bytes_min = base_bytes_max / config.max_bytes_for_level_multiplier;

            let mut cur_level_size = table_ctx.max_level_size;
            for _ in table_ctx.base_level..config.max_level as usize {
                cur_level_size /= config.max_bytes_for_level_multiplier;
            }

            let base_level_size = if cur_level_size <= base_bytes_min {
                // Case 1. If we make target size of last level to be max_level_size,
                // target size of the first non-empty level would be smaller than
                // base_bytes_min. We set it be base_bytes_min.
                base_bytes_min + 1
            } else {
                while table_ctx.base_level > 1 && cur_level_size > base_bytes_max {
                    table_ctx.base_level -= 1;
                    cur_level_size /= config.max_bytes_for_level_multiplier;
                }
                std::cmp::min(base_bytes_max, cur_level_size)
            };

            let level_multiplier = config.max_bytes_for_level_multiplier as f64;
            let mut level_size = base_level_size;
            for i in table_ctx.base_level..=config.max_level as usize {
                table_ctx.level_max_bytes[i] = std::cmp::max(level_size, base_bytes_max);
                level_size = (level_size as f64 * level_multiplier) as u64;
            }
        }

        let idle_file_count = levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>()
            - handlers[0].get_pending_file_count();
        let max_l0_score = std::cmp::max(
            SCORE_BASE * 2,
            levels.l0.as_ref().unwrap().sub_levels.len() as u64 * SCORE_BASE
                / config.level0_tier_compact_file_number,
        );
        let total_size = levels.l0.as_ref().unwrap().total_file_size
            - handlers[0].get_output_pending_file_size();
        if idle_file_count > 0 {
            // trigger intra-l0 compaction at first when the number of files is too large.
            let l0_score =
                idle_file_count as u64 * SCORE_BASE / config.level0_tier_compact_file_number;
            ctx.score_levels.push(TableLevel {
                table_id: 0,
                target_level: 0,
                select_level: 0,
                score: std::cmp::min(l0_score, max_l0_score),
            });
        }

        for (table_id, table_ctx) in &mut ctx.table_infos {
            let score = total_size * SCORE_BASE / config.max_bytes_for_level_base;
            ctx.score_levels.push(TableLevel {
                table_id: *table_id,
                target_level: table_ctx.base_level,
                select_level: 0,
                score,
            });
            for level in &levels.levels {
                let level_idx = level.level_idx as usize;
                if level_idx < table_ctx.base_level || level_idx >= config.max_level as usize {
                    continue;
                }
                let total_size = table_ctx.level_bytes[level_idx].wrapping_sub(
                    handlers[level_idx].get_target_table_pending_file_size(*table_id),
                );
                if total_size == 0 {
                    continue;
                }
                ctx.score_levels.push(TableLevel {
                    score: total_size * SCORE_BASE / table_ctx.level_max_bytes[level_idx],
                    select_level: level_idx,
                    target_level: level_idx + 1,
                    table_id: *table_id,
                });
            }
        }
        ctx.score_levels.sort_by(|a, b| b.score.cmp(&a.score));
        Some(ctx)
    }
}

impl LevelSelector for SplitTableLevelSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        selector_stats: &mut LocalSelectorStatistic,
    ) -> Option<CompactionTask> {
        let overlap = create_overlap_strategy(group.compaction_config.compaction_mode());
        let ctx =
            match self.calculate_level_scores(&group.compaction_config, levels, level_handlers) {
                None => {
                    return DynamicLevelSelector::default().pick_compaction(
                        task_id,
                        group,
                        levels,
                        level_handlers,
                        selector_stats,
                    )
                }
                Some(ctx) => ctx,
            };
        for table_info in ctx.score_levels {
            if table_info.score <= SCORE_BASE {
                return None;
            }
            let mut stats = LocalPickerStatistic::default();
            let input = if table_info.select_level == 0 && table_info.target_level == 0 {
                assert_eq!(table_info.table_id, 0);
                let mut picker =
                    TierCompactionPicker::new(group.compaction_config.clone(), overlap.clone());
                picker.pick_compaction(levels, level_handlers, &mut stats)
            } else if table_info.select_level == 0 {
                assert!(table_info.table_id > 0);
                let mut picker = LevelCompactionPicker::new(
                    table_info.target_level,
                    Some(table_info.table_id),
                    group.compaction_config.clone(),
                    overlap.clone(),
                );
                picker.pick_compaction(levels, level_handlers, &mut stats)
            } else {
                let mut picker = MinOverlappingPicker::new(
                    table_info.select_level,
                    table_info.target_level,
                    group.compaction_config.max_bytes_for_level_base,
                    Some(table_info.table_id),
                    overlap.clone(),
                );
                picker.pick_compaction(levels, level_handlers, &mut stats)
            };
            if let Some(ret) = input {
                ret.add_pending_task(task_id, table_info.table_id, level_handlers);
                return Some(create_compaction_task(
                    group.compaction_config.as_ref(),
                    ret,
                    ctx.inner.base_level,
                    self.task_type(),
                ));
            }
        }
        None
    }

    fn name(&self) -> &'static str {
        "SplitTableLevelSelector"
    }

    fn task_type(&self) -> TaskType {
        TaskType::Dynamic
    }
}
