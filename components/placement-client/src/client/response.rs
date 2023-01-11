use model::range::PartitionRange;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Status {
    OK,
    Cancelled,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

#[derive(Debug, Clone)]
pub(crate) enum Response {
    Heartbeat {
        status: Status,
    },
    ListRange {
        status: Status,
        ranges: Option<Vec<PartitionRange>>,
    },
}
