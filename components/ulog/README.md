We are currently using log4rs crate for range-server and env_logger for testing. `log4rs` and `slog` works well for multi-threaded applications; but they are not good fit for our thread-per-core scenario.

Ultimate goal of this crate is creating a logging binder that using io-uring file to write log records, making logging completely asynchronous.
