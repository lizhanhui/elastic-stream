pub mod rpc_generated;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::rpc_generated::elastic::store::{ListRange, ListRangeArgs};

    #[test]
    fn test_list_range() -> Result<(), Box<dyn Error>> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let list_range = ListRange::create(&mut builder, &ListRangeArgs { partition_id: 1 });
        builder.finish(list_range, None);
        let buf = builder.finished_data();

        let list_range_header = super::rpc_generated::elastic::store::root_as_list_range(buf)?;
        assert_eq!(1, list_range_header.partition_id());
        Ok(())
    }
}
