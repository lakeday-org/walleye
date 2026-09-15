//! Account retained Arrow allocations once, including shared buffers across batches.
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct ResultMemory {
    allocations: HashMap<usize, usize>,
}
impl ResultMemory {
    pub(crate) fn charge(&mut self, batch: &RecordBatch) -> usize {
        std::mem::size_of::<RecordBatch>()
            + batch
                .columns()
                .iter()
                .map(|a| self.array(&a.to_data()))
                .sum::<usize>()
    }
    fn array(&mut self, data: &ArrayData) -> usize {
        let mut bytes = std::mem::size_of::<ArrayData>();
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            // Custom/external allocations report zero capacity. Charge the observed
            // extent conservatively, growing it when a later slice reveals more.
            let size = buffer
                .capacity()
                .max(buffer.ptr_offset().saturating_add(buffer.len()));
            let previous = self
                .allocations
                .entry(buffer.data_ptr().as_ptr() as usize)
                .or_default();
            bytes += size.saturating_sub(*previous) + std::mem::size_of_val(buffer);
            *previous = (*previous).max(size);
        }
        bytes
            + data
                .child_data()
                .iter()
                .map(|child| self.array(child))
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use std::sync::Arc;

    #[test]
    fn many_slices_charge_shared_allocations_once() {
        let values = StringArray::from(vec!["x".repeat(256); 8192]);
        let batch = RecordBatch::try_from_iter(vec![("value", Arc::new(values) as _)]).unwrap();
        let slices: Vec<_> = (0..128).map(|i| batch.slice(i * 64, 64)).collect();
        assert!(
            slices
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>()
                > 8 * 1024 * 1024
        );
        let mut memory = ResultMemory::default();
        let retained: usize = slices.iter().map(|batch| memory.charge(batch)).sum();
        assert!(retained >= 2 * 1024 * 1024);
        assert!(retained < 3 * 1024 * 1024);
    }

    #[test]
    fn independent_allocations_are_not_deduplicated() {
        let batches: Vec<_> = (0..8)
            .map(|_| {
                RecordBatch::try_from_iter(vec![(
                    "value",
                    Arc::new(StringArray::from(vec!["x".repeat(256); 8192])) as _,
                )])
                .unwrap()
            })
            .collect();
        let mut memory = ResultMemory::default();
        assert!(
            batches
                .iter()
                .map(|batch| memory.charge(batch))
                .sum::<usize>()
                > 16 * 1024 * 1024
        );
    }
}
