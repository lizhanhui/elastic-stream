# Threading Model

## Range Server Threading Model
Primary design goal of range-server is to scale linearly with addition of hardware: including CPU and SSD. To achieve this, threads of it must be as independent from one another as possible, to avoid software locks and even atomic instructions. With that said, instead of centralizing shared data in a global location that all threads access after acquiring a lock, each thread of range-server has its own data. When other threads want to access the data, they pass a message to the owning thread to perform the operation on their behalf through lockless ring-buffers.

Range Server has 3 categories of threads: worker threads, IO thread and miscellaneous auxiliary threads. The former two kinds follows [Thread-per-Core](https://www.datadoghq.com/blog/engineering/introducing-glommio/) architecture while auxiliary threads use traditional multi-threads parallelism, with CPU affinity set.


![Threading Model](images/threading_model.png)

Each range-server, by default, has one IO thread, taking up a dedicated core, running an io_uring instance with SQ_POLLING and IO_POLLING. This configuration offers best [latency performance](docs/benchmark.md) and makes most the [modern storage capabilities](https://atlarge-research.com/pdfs/2022-systor-apis.pdf).

Multiple independent workers runs in parallel within a range-server. Each of them are backed by an io_uring instance with FAST_POLL feature set. A worker also takes up a dedicated core and is in charge of a group of streams. Inter-worker communication is fulfilled by way of message passing, aka, lockless ring buffer.
