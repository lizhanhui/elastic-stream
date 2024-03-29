// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate controls the global allocator used by ElasticStream.
//!
//! With few exceptions, _every binary and project in the workspace
//! should link (perhaps transitively) to _alloc_. This is to ensure
//! that tests and benchmarks run with the production allocator.
//!
//! At present all Unixes use jemalloc, and others don't. Whichever
//! allocator is used, this crate presents the same API, and some
//! profiling functions become no-ops. Note however that _not all
//! platforms override C malloc, including macOS_. This means on some
//! platforms RocksDB is using the system malloc. On Linux C malloc is
//! redirected to jemalloc.
//!
//! This crate accepts five cargo features:
//!
//! - mem-profiling - compiles jemalloc and this crate with profiling capability
//!
//! - jemalloc - compiles tikv-jemallocator (default)
//!
//! - tcmalloc - compiles tcmalloc
//!
//! - mimalloc - compiles mimalloc
//!
//! - snmalloc - compiles snmalloc
//!
//! Ideally there should be no jemalloc-specific code outside this
//! crate.
//!
//! # Profiling
//!
//! Profiling with jemalloc requires both build-time and run-time
//! configuration. At build time cargo needs the `--mem-profiling`
//! feature, and at run-time jemalloc needs to set the `opt.prof`
//! option to true, aka `MALLOC_CONF="opt.prof:true"`.
//!
//! In production you might also set `opt.prof_active` to `false` to
//! keep profiling off until there's an incident. Jemalloc has
//! a variety of run-time [profiling options].
//!
//! [profiling options]: http://jemalloc.net/jemalloc.3.html#opt.prof
//!
//! Here's an example of how you might build and run via cargo, with
//! profiling:
//!
//! ```notrust
//! export MALLOC_CONF="prof:true,prof_active:false,prof_prefix:$(pwd)/jeprof"
//! cargo test --features mem-profiling -p alloc -- --ignored
//! ```
//!
//! (In practice you might write this as a single statement, setting
//! `MALLOC_CONF` only temporarily, e.g. `MALLOC_CONF="..." cargo test
//! ...`).
//!
//! When running cargo while `prof:true`, you will see messages like
//!
//! ```notrust
//! <jemalloc>: Invalid conf pair: prof:true
//! <jemalloc>: Invalid conf pair: prof_active:false
//! ```
//!
//! This is normal - they are being emitting by the jemalloc in cargo
//! and rustc, which are both configured without profiling.

#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(custom_test_frameworks))]
#![cfg_attr(test, test_runner(runner::run_env_conditional_tests))]
#![feature(core_intrinsics)]

#[cfg(feature = "jemalloc")]
#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod trace;

#[cfg(not(all(unix, feature = "jemalloc")))]
mod default;

pub type AllocStats = Vec<(&'static str, usize)>;

// Allocators
#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;
#[cfg(all(unix, feature = "tcmalloc"))]
#[path = "tcmalloc.rs"]
mod imp;
#[cfg(all(unix, feature = "mimalloc"))]
#[path = "mimalloc.rs"]
mod imp;
#[cfg(all(unix, feature = "snmalloc"))]
#[path = "snmalloc.rs"]
mod imp;
#[cfg(not(all(
    unix,
    any(
        feature = "jemalloc",
        feature = "tcmalloc",
        feature = "mimalloc",
        feature = "snmalloc"
    )
)))]
#[path = "system.rs"]
mod imp;

pub use crate::{imp::*, trace::*};

#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();

#[cfg(test)]
mod runner {
    extern crate test;
    use test::*;

    /// Check for ignored test cases with ignore message "#ifdef <VAR_NAME>".
    /// The test case will be enabled if the specific environment variable
    /// is set.
    pub fn run_env_conditional_tests(cases: &[&TestDescAndFn]) {
        let cases: Vec<_> = cases
            .iter()
            .map(|case| {
                let mut desc = case.desc.clone();
                let testfn = match case.testfn {
                    TestFn::StaticTestFn(f) => TestFn::StaticTestFn(f),
                    TestFn::StaticBenchFn(f) => TestFn::StaticBenchFn(f),
                    ref f => panic!("unexpected testfn {:?}", f),
                };
                if let Some(msg) = desc.ignore_message {
                    let keyword = "#ifdef";
                    if let Some(s) = msg.strip_prefix(keyword) {
                        let var_name = s.trim();
                        if var_name.is_empty() || std::env::var(var_name).is_ok() {
                            desc.ignore = false;
                            desc.ignore_message = None;
                        }
                    }
                }
                TestDescAndFn { desc, testfn }
            })
            .collect();
        let args = std::env::args().collect::<Vec<_>>();
        test_main(&args, cases, None)
    }
}
