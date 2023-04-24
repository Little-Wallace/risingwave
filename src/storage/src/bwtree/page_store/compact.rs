use risingwave_pb::hummock::CompactTask;

use crate::hummock::HummockResult;

pub struct CompactorRunner {
    task: CompactTask,
}

impl CompactorRunner {
    pub async fn run(&mut self) -> HummockResult<()> {
        let segment = self.task.input_ssts.iter();
        Ok(())
    }
}
