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

#[derive(Debug, Clone, Copy)]
pub(crate) enum Response {
    ListRange { status: Status },
}
