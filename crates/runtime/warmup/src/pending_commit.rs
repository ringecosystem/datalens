use datalens_core::LedgerRange;
use datalens_writer::DurableWriteResult;

pub(crate) struct PendingWarmupCommit {
    pub(crate) range: LedgerRange,
    pub(crate) fetched_ranges: u64,
    pub(crate) written_ranges: u64,
    pub(crate) empty_ranges: u64,
    pub(crate) provider_calls: u64,
    pub(crate) rows_fetched: usize,
}

impl PendingWarmupCommit {
    pub(crate) fn include_flush_result(&mut self, result: &DurableWriteResult) {
        self.written_ranges += (result.data_objects.len() + result.empty_coverages.len()) as u64;
        self.empty_ranges += result.empty_coverages.len() as u64;
    }
}
