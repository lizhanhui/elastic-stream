# Developer Guide

## How to Build

This project employs several distinct programming languages. The Placement Driver is developed utilizing Go, whereas the Range Server leverages the Rust language. In addition, we furnish two SDK implementations with Java and Rust.

For the parts crafted in Go and Java, you can locate the corresponding build commands in the `pd` and `sdks/frontend-java` directories respectively.


## Run with Address Sanitizer

Sometimes you have to deal with low-level operations, for example, interacting with DMA requires page alignment memory. Unsafe code is required to handle these cases and address sanitizer would be helpful to maintain memory safety.

```sh
RUSTFLAGS=-Zsanitizer=address cargo test test_layout -Zbuild-std --target x86_64-unknown-linux-gnu
```
Read the [following link](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html) for more advanced usage.
