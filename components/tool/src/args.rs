use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from("/data/data0"))]
    pub path: String,

    /// Queue depth
    #[arg(short, long, default_value_t = 32)]
    pub qd: u32,

    /// Block size
    #[arg(short, long, default_value_t = 4096)]
    pub bs: u16,

    /// I/O size in block-size
    #[arg(short, long, default_value_t = 64)]
    pub io_size: usize,

    /// File size in Gigabytes
    #[arg(short, long, default_value_t = 1)]
    pub file_size: usize,
}
