use clap::Parser;

use hyper::{body::HttpBody, Client};
use tokio::{
    io::{stdout, AsyncWriteExt},
    time,
};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 9898)]
    port: u16,

    #[arg(short, long, default_value_t = 1000)]
    duration: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!(
        "Host: {}, Port: {}, Duration: {}ms",
        args.host, args.port, args.duration
    );
    loop {
        time::sleep(time::Duration::from_millis(args.duration)).await;
        let client = Client::new();
        let uri = format!("http://{}:{}/metrics", args.host, args.port);
        let uri = uri.parse().unwrap();
        let mut resp = client.get(uri).await.unwrap();
        while let Some(chunk) = resp.body_mut().data().await {
            stdout().write_all(&chunk.unwrap()).await.unwrap();
        }
    }
}
