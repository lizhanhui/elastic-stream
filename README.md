## How to Build

## Collect and Report Unit Test Coverage

```sh
cargo llvm-cov
```

With HTML report,
```sh
cargo llvm-cov --html
```
or 
```sh
cargo llvm-cov --open
```

If wishing to execute `cargo run` instead of `cargo test`, use run sub-command:

```sh
cargo llvm-cov run
```

Read [more](https://crates.io/crates/cargo-llvm-cov)

Sample [integration](https://github.com/taiki-e/cargo-llvm-cov) with github action.

## How to Contribute

## Notes

### **communicating-between-sync-and-async-code**
`Store` module is built on top of io-uring directly. The `Server` module, however, is built using `tokio-uring`, following thread-per-core paradigm, which as a result is fully async. Reading and writing records between these two modules involve communication between async and sync code, as shall comply with [the following guideline](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html#communicating-between-sync-and-async-code) 

## Run ping-pong

### Launch Ping-Pong Server
```sh
cargo run --bin data-node
```

### Run Ping-Pong client
```sh
cargo run --bin ping-pong
```
