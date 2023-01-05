#[derive(Debug)]
pub(crate) enum Request {
    ListRange { partition_id: i64 },

    Heartbeat,
}
