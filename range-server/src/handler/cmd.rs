use super::{
    append::Append, create_range::CreateRange, fetch::Fetch, heartbeat::Heartbeat, ping::Ping,
    seal_range::SealRange,
};
use crate::range_manager::RangeManager;
use codec::frame::Frame;
use log::error;
use protocol::rpc::header::{ErrorCode, OperationCode};
use std::{fmt, rc::Rc};

#[derive(Debug)]
pub(crate) enum Command<'a> {
    Append(Append),
    Fetch(Fetch<'a>),
    CreateRange(CreateRange<'a>),
    SealRange(SealRange<'a>),
    Ping(Ping<'a>),
    Heartbeat(Heartbeat<'a>),
}

impl<'a> Command<'a> {
    pub fn from_frame(frame: &Frame) -> Result<Command, ErrorCode> {
        match frame.operation_code {
            OperationCode::UNKNOWN => Err(ErrorCode::UNSUPPORTED_OPERATION),

            OperationCode::PING => Ok(Command::Ping(Ping::new(frame))),

            OperationCode::GOAWAY => {
                error!("GoAway is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::HEARTBEAT => Ok(Command::Heartbeat(Heartbeat::parse_frame(frame)?)),

            OperationCode::ALLOCATE_ID => {
                error!("AllocateId is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::APPEND => Ok(Command::Append(Append::parse_frame(frame)?)),

            OperationCode::FETCH => Ok(Command::Fetch(Fetch::parse_frame(frame)?)),

            OperationCode::LIST_RANGE => {
                error!("ListRange is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::CREATE_RANGE => {
                Ok(Command::CreateRange(CreateRange::parse_frame(frame)?))
            }

            OperationCode::SEAL_RANGE => Ok(Command::SealRange(SealRange::parse_frame(frame)?)),

            OperationCode::SYNC_RANGE => Err(ErrorCode::UNSUPPORTED_OPERATION),

            OperationCode::CREATE_STREAM => {
                error!("CreateStream is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::DELETE_STREAM => {
                error!("DeleteStream is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::UPDATE_STREAM => {
                error!("UpdateStream is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::DESCRIBE_STREAM => {
                error!("DescribeStream is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }
            OperationCode::TRIM_STREAM => {
                error!("TrimStream is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::REPORT_METRICS => {
                error!("ReportMetrics is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::DESCRIBE_PLACEMENT_DRIVER => {
                error!("DescribePlacementDriver is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            OperationCode::REPORT_REPLICA_PROGRESS => {
                error!("ReportRangeProgress is not supported in range-server");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }

            _ => {
                error!("Request with unsupported operation code  is received");
                Err(ErrorCode::UNSUPPORTED_OPERATION)
            }
        }
    }

    pub(crate) async fn apply<M>(&self, range_manager: Rc<M>, response: &mut Frame)
    where
        M: RangeManager,
    {
        match self {
            Command::Append(cmd) => cmd.apply(range_manager, response).await,
            Command::Fetch(cmd) => cmd.apply(range_manager, response).await,
            Command::Heartbeat(cmd) => cmd.apply(range_manager, response).await,
            Command::Ping(cmd) => cmd.apply(range_manager, response).await,
            Command::CreateRange(cmd) => cmd.apply(range_manager, response).await,
            Command::SealRange(cmd) => cmd.apply(range_manager, response).await,
        }
    }
}

impl<'a> fmt::Display for Command<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Append(cmd) => write!(f, "{}", cmd),
            Command::Fetch(cmd) => write!(f, "{}", cmd),
            Command::Heartbeat(cmd) => write!(f, "{}", cmd),
            Command::Ping(cmd) => write!(f, "{}", cmd),
            Command::CreateRange(cmd) => write!(f, "{}", cmd),
            Command::SealRange(cmd) => write!(f, "{}", cmd),
        }
    }
}
