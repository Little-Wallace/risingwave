use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    level_delete_ssts, level_insert_ssts, split_base_levels, summarize_group_deltas,
    GroupDeltasSummary,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::{CompactionGroupId, HummockSstableId};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    HummockVersion, HummockVersionDelta, Level, LevelType, OverlappingLevel, SstableInfo,
};

#[derive(Clone)]
pub struct LocalGroup {
    pub levels: Vec<Level>,
    pub l0: OverlappingLevel,
}

impl LocalGroup {
    pub fn new(max_level: u32) -> Self {
        let mut levels = Vec::with_capacity(max_level as usize);
        for level_idx in 1..=max_level {
            levels.push(Level {
                level_idx,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
            });
        }
        Self {
            levels,
            l0: OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            },
        }
    }

    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary, local_related_only: bool) {
        let GroupDeltasSummary {
            delete_sst_levels,
            delete_sst_ids_set,
            insert_sst_level_id,
            insert_sub_level_id,
            insert_table_infos,
            ..
        } = summary;
        let mut deleted = false;
        for level_idx in &delete_sst_levels {
            if *level_idx == 0 {
                for level in &mut self.l0.sub_levels {
                    deleted = level_delete_ssts(level, &delete_sst_ids_set) || deleted;
                }
            } else {
                let idx = *level_idx as usize - 1;
                deleted = level_delete_ssts(&mut self.levels[idx], &delete_sst_ids_set) || deleted;
            }
        }
        if local_related_only && !delete_sst_ids_set.is_empty() && !deleted {
            // If no sst is deleted, the current delta will not be related to the local version.
            // Therefore, if we only care local related data, we can return without inserting the
            // ssts.
            return;
        }
        if !insert_table_infos.is_empty() {
            if insert_sst_level_id == 0 {
                let index = self
                    .l0
                    .sub_levels
                    .partition_point(|level| level.sub_level_id < insert_sub_level_id);
                if local_related_only {
                    // Some sub level in the full hummock version may be empty in the local related
                    // pruned version, so it's possible the level to be inserted is not found
                    if index == self.l0.sub_levels.len()
                        || self.l0.sub_levels[index].sub_level_id > insert_sub_level_id
                    {
                        // level not found, insert a new level
                        let new_level = new_sub_level(
                            insert_sub_level_id,
                            LevelType::Nonoverlapping,
                            insert_table_infos,
                        );
                        self.l0.sub_levels.insert(index, new_level);
                    } else {
                        // level found, add to the level.
                        level_insert_ssts(&mut self.l0.sub_levels[index], insert_table_infos);
                    }
                } else {
                    assert!(
                        index < self.l0.sub_levels.len() && self.l0.sub_levels[index].sub_level_id == insert_sub_level_id,
                        "should find the level to insert into when applying compaction generated delta. sub level idx: {}",
                        insert_sub_level_id
                    );
                    level_insert_ssts(&mut self.l0.sub_levels[index], insert_table_infos);
                }
            } else {
                let idx = insert_sst_level_id as usize - 1;
                level_insert_ssts(&mut self.levels[idx], insert_table_infos);
            }
        }
        if delete_sst_levels.iter().any(|level_id| *level_id == 0) {
            self.l0
                .sub_levels
                .retain(|level| !level.table_infos.is_empty());
            self.l0.total_file_size = self
                .l0
                .sub_levels
                .iter()
                .map(|level| level.total_file_size)
                .sum::<u64>();
        }
    }
}

#[derive(Clone)]
pub struct LocalHummockVersion {
    pub id: u64,
    pub groups: HashMap<u64, LocalGroup>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
}

impl LocalHummockVersion {
    pub fn new(
        id: u64,
        group: HashMap<u64, LocalGroup>,
        max_committed_epoch: u64,
        safe_epoch: u64,
    ) -> Self {
        Self {
            id,
            groups: group,
            max_committed_epoch,
            safe_epoch,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_compaction_group_levels(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> &LocalGroup {
        self.groups
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    pub fn get_compaction_levels(&self) -> Vec<LocalGroup> {
        self.groups.values().cloned().collect_vec()
    }

    pub fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut LocalGroup {
        self.groups
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    pub fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize {
        // l0 is currently separated from all levels
        self.groups
            .get(&compaction_group_id)
            .map(|group| group.levels.len() + 1)
            .unwrap_or(0)
    }

    fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> Vec<(HummockSstableId, u64, u32)> {
        let mut split_id_vers = vec![];
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId
            || !self.groups.contains_key(&parent_group_id)
        {
            return split_id_vers;
        }
        let [parent_levels, cur_levels] = self
            .groups
            .get_many_mut([&parent_group_id, &group_id])
            .unwrap();
        for sub_level in &mut parent_levels.l0.sub_levels {
            let mut insert_table_infos = vec![];
            for table_info in &mut sub_level.table_infos {
                if table_info
                    .table_ids
                    .iter()
                    .any(|table_id| member_table_ids.contains(table_id))
                {
                    table_info.divide_version += 1;
                    split_id_vers.push((table_info.get_id(), table_info.get_divide_version(), 0));
                    let mut branch_table_info = table_info.clone();
                    branch_table_info.table_ids = table_info
                        .table_ids
                        .drain_filter(|table_id| member_table_ids.contains(table_id))
                        .collect_vec();
                    insert_table_infos.push(branch_table_info);
                }
            }
            add_new_sub_level(
                &mut cur_levels.l0,
                sub_level.sub_level_id,
                sub_level.level_type(),
                insert_table_infos,
            );
        }
        split_id_vers.extend(split_base_levels(
            member_table_ids,
            &mut parent_levels.levels,
            &mut cur_levels.levels,
        ));
        split_id_vers
    }

    pub fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId> {
        let mut ret = HashMap::new();
        for (compaction_group_id, levels) in &self.groups {
            for sub_level in &levels.l0.sub_levels {
                for table_info in &sub_level.table_infos {
                    table_info.table_ids.iter().for_each(|table_id| {
                        ret.insert(TableId::new(*table_id), *compaction_group_id);
                    });
                }
            }
            for level in &levels.levels {
                for table_info in &level.table_infos {
                    table_info.table_ids.iter().for_each(|table_id| {
                        ret.insert(TableId::new(*table_id), *compaction_group_id);
                    });
                }
            }
        }
        ret
    }

    pub fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            if let Some(group_construct) = &summary.group_construct {
                self.groups.insert(
                    *compaction_group_id,
                    LocalGroup::new(
                        group_construct.group_config.as_ref().unwrap().max_level as u32,
                    ),
                );
                let parent_group_id = group_construct.parent_group_id;
                self.init_with_parent_group(
                    parent_group_id,
                    *compaction_group_id,
                    &HashSet::from_iter(group_construct.get_table_ids().iter().cloned()),
                );
            }
            let has_destroy = summary.group_destroy.is_some();
            let levels = self
                .groups
                .get_mut(compaction_group_id)
                .expect("compaction group should exist");

            assert!(
                self.max_committed_epoch <= version_delta.max_committed_epoch,
                "new max commit epoch {} is older than the current max commit epoch {}",
                version_delta.max_committed_epoch,
                self.max_committed_epoch
            );
            if self.max_committed_epoch < version_delta.max_committed_epoch {
                // `max_committed_epoch` increases. It must be a `commit_epoch`
                let GroupDeltasSummary {
                    delete_sst_levels,
                    delete_sst_ids_set,
                    insert_sst_level_id,
                    insert_sub_level_id,
                    insert_table_infos,
                    ..
                } = summary;
                assert!(
                    insert_sst_level_id == 0 || insert_table_infos.is_empty(),
                    "we should only add to L0 when we commit an epoch. Inserting into {} {:?}",
                    insert_sst_level_id,
                    insert_table_infos
                );
                assert!(
                    delete_sst_levels.is_empty() && delete_sst_ids_set.is_empty() || has_destroy,
                    "no sst should be deleted when committing an epoch"
                );
                add_new_sub_level(
                    &mut levels.l0,
                    insert_sub_level_id,
                    LevelType::Overlapping,
                    insert_table_infos,
                );
            } else {
                // `max_committed_epoch` is not changed. The delta is caused by compaction.
                levels.apply_compact_ssts(summary, false);
            }
            if has_destroy {
                self.groups.remove(compaction_group_id);
            }
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
    }
}

impl From<HummockVersion> for LocalHummockVersion {
    fn from(version: HummockVersion) -> LocalHummockVersion {
        let mut groups = HashMap::with_capacity(version.levels.len());
        for (group_id, levels) in version.levels {
            groups.insert(
                group_id,
                LocalGroup {
                    levels: levels.levels,
                    l0: levels.l0.unwrap(),
                },
            );
        }
        Self {
            id: version.id,
            groups,
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
        }
    }
}

impl From<&LocalHummockVersion> for HummockVersion {
    fn from(version: &LocalHummockVersion) -> Self {
        let mut levels = HashMap::default();
        for (group_id, group) in &version.groups {
            levels.insert(
                *group_id,
                Levels {
                    levels: group.levels.clone(),
                    l0: Some(group.l0.clone()),
                },
            );
        }

        Self {
            id: version.id,
            levels,
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.safe_epoch,
        }
    }
}

pub fn add_new_sub_level(
    l0: &mut OverlappingLevel,
    insert_sub_level_id: u64,
    level_type: LevelType,
    insert_table_infos: Vec<SstableInfo>,
) {
    if insert_sub_level_id == u64::MAX {
        return;
    }
    if let Some(newest_level) = l0.sub_levels.last() {
        assert!(
            newest_level.sub_level_id < insert_sub_level_id,
            "inserted new level is not the newest: prev newest: {}, insert: {}. L0: {:?}",
            newest_level.sub_level_id,
            insert_sub_level_id,
            l0,
        );
    }
    // All files will be committed in one new Overlapping sub-level and become
    // Nonoverlapping  after at least one compaction.
    let level = new_sub_level(insert_sub_level_id, level_type, insert_table_infos);
    l0.total_file_size += level.total_file_size;
    l0.sub_levels.push(level);
}

fn new_sub_level(sub_level_id: u64, level_type: LevelType, table_infos: Vec<SstableInfo>) -> Level {
    let total_file_size = table_infos.iter().map(|table| table.file_size).sum();
    Level {
        level_idx: 0,
        level_type: level_type as i32,
        table_infos,
        total_file_size,
        sub_level_id,
    }
}
