pub trait Batch {
    /// Base offset of record batch
    fn offset(&self) -> u64;

    /// Quantity of the internal nested records
    fn len(&self) -> u32;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::Batch;

    struct TestBatch;

    impl Batch for TestBatch {
        fn offset(&self) -> u64 {
            0
        }

        fn len(&self) -> u32 {
            0
        }
    }

    #[test]
    fn test_batch_is_empty() {
        let batch = TestBatch {};
        assert!(batch.is_empty());
        assert_eq!(0, batch.offset());
    }
}
