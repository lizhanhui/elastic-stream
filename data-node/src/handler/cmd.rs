use std::rc::Rc;

use codec::frame::{Frame, OperationCode};
use protocol::rpc::header::ErrorCode;
use slog::Logger;
use store::ElasticStore;

use super::{describe_range::DescribeRange, seal_range::SealRange};

#[derive(Debug)]
pub(crate) enum Command<'a> {
    DescribeRange(DescribeRange<'a>),
    SealRange(SealRange<'a>),
}

impl<'a> Command<'a> {
    pub fn from_frame(logger: Logger, frame: &Frame) -> Result<Command, ErrorCode> {
        match frame.operation_code {
            OperationCode::DescribeRanges => Ok(Command::DescribeRange(
                DescribeRange::parse_frame(logger.clone(), frame)?,
            )),
            OperationCode::Unknown => todo!(),
            OperationCode::Ping => todo!(),
            OperationCode::GoAway => todo!(),
            OperationCode::Heartbeat => todo!(),
            OperationCode::Append => todo!(),
            OperationCode::Fetch => todo!(),
            OperationCode::ListRanges => todo!(),

            OperationCode::SealRanges => Ok(Command::SealRange(SealRange::parse_frame(
                logger.clone(),
                frame,
            )?)),

            OperationCode::SyncRanges => todo!(),
            OperationCode::CreateStreams => todo!(),
            OperationCode::DeleteStreams => todo!(),
            OperationCode::UpdateStreams => todo!(),
            OperationCode::DescribeStreams => todo!(),
            OperationCode::TrimStreams => todo!(),
            OperationCode::ReportMetrics => todo!(),
        }
    }

    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {
        match self {
            Command::DescribeRange(cmd) => cmd.apply(store, response).await,
            Command::SealRange(cmd) => cmd.apply(store, response).await,
        }
    }
}
