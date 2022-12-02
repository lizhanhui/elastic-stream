use clap::Parser;
use codec::frame::{Frame, HeaderFormat, OperationCode};
use monoio::net::TcpStream;
use slog::{error, info, o, Drain, Logger};

#[monoio::main]
async fn main() {
    let args = Args::parse();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    launch(&args, log).await
}

pub const DEFAULT_DATA_NODE_PORT: u16 = 10911;

#[derive(Debug, Parser)]
#[clap(name = "ping_pong", author, version, about, long_about = None)]
struct Args {
    #[clap(name = "hostname", short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    #[clap(short, long, default_value_t = DEFAULT_DATA_NODE_PORT)]
    port: u16,
}

async fn launch(args: &Args, logger: Logger) {
    let connect = format!("{}:{}", args.host, args.port);
    info!(logger, "Start to connect to {connect}");

    let stream = match TcpStream::connect(&connect).await {
        Ok(stream) => {
            info!(logger, "Connected to {connect:?}");
            stream
        }
        Err(e) => {
            error!(logger, "Failed to connect to {connect:?}. Cause: {e:#?}");
            return;
        }
    };

    let frame = Frame {
        operation_code: OperationCode::Ping,
        flag: 0u8,
        stream_id: 1,
        header_format: HeaderFormat::FlatBuffer,
        header: None,
        payload: None,
    };

    let mut buffer = bytes::BytesMut::with_capacity(4 * 1024);
    frame.encode(&mut buffer).unwrap_or_else(|e| {
        error!(logger, "Failed to encode frame. Cause: {e:#?}");
    });

    // let mut connection =
}
