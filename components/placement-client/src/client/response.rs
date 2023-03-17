use model::range::StreamRange;

use model::Status;

#[derive(Debug, Clone)]
pub enum Response {
    Heartbeat {
        status: Status,
    },
    ListRange {
        status: Status,
        ranges: Option<Vec<StreamRange>>,
    },
}
