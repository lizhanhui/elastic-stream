use flatc_rust;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=fbs/header.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("fbs/header.fbs")],
        out_dir: Path::new("src/generated/"),
        ..Default::default()
    })
    .expect("flatc");
}
