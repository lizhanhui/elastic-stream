## Cloud Native Design

As technology advances and data continues to explode, traditional messaging and streaming platforms have exhibited their limitations in terms of infrastructure cost and operational management complexity. To address these challenges and make most of technology innovations, now is the time to redesign streaming platform, to meet following desirable expectations:

* Cost Effective - Spend every dollar on demand for the most cost-effective service in the market;
* Efficiency - Performance scales linearly with addition of resources, offering response latency in milliseconds;
* Autonomous - Automatic failover and service re-balance;

Separation of concerns is one of endorsed principles. Our design and this article also follow this rule.

## Cost
Cloud computing has become the mainstream. Among those cloud products, Object Storage Service impacts the storage landscape significantly. Compared to traditional storage products, object storage has several advantages:

* No operational cost for managing large scale storage cluster;
* Out-of-box usability;
* Scalability to unlimited capacity;
* 10x lower storage cost per GiB/month(EC, tiering storage);
* Billing by the actual amount of storage;
* Up to 1 GiB/s throughput per client;
* Multi-AZ availability

Survey shows that storage cost takes up to 60%~80% percent of the total infrastructure cost for a typical streaming deployment whilst compute and network costs share the rest. Offloading data to object storage alone would reduce total cost by 50% ~ 70%.

Another advantage to emphasize is that object storage is [multiple AZ](https://aws.amazon.com/s3/faqs/?nc1=h_ls). This saves significant network transfer costs for most use cases within delay in seconds.

Design are all about trade-off and object storage is also designed with inherent limitations:
* Limited IOPS;
* Poor metadata performance;

In addition to pay-as-you-go object storage, cloud computing also offers scalability for compute resources, ranging from function, container to ECS. This scalability lays an foundation to cut out traditional resource over-commitment and saves operators from resource planning. Given that an absolute majority of the enterprise services has only a few busy hours, compute costs are reduced at least by half if going serverless.

## Efficiency

Besides innovations in the cloud, modern software and hardware provide capabilities that radically impact software architecture and eventual product performance. Take typical enterprise SSD, Samsung PM1743, as example:

| Spec            | Value       |
| ----------------| ----------- |
| Sequential Read | 13,000 MB/s |
| Random Read     | 2,500K IOPS |
| Sequential Write| 6,600 MB/s  |
| Random Write    | 250K IOPS   |

After comparing with widely known I/O specs of HDDs, apparently, fundamentals have changed:
* SSD serves millions of IOPS while HDD is generally some 200 IOPS;
* Sequential throughput, both read and write, is only 2x of their random counterparts for SSD; HDD, however, is generally 100x;
* High queue depth(some 32) is required to fully utilize I/O capabilities of SSD; while one I/O thread suffices for HDD;

### Thread-per-Core

Moderately complex application has many tasks to perform. For instance, it may need to read data from a database, feed that data into specific business process pipelines, and then call other internal services and aggregate all intermediate results to generate final outcome. Some of those tasks are naturally sequential, but many can be done in parallel. Considering modern hardware offers:

* an increasing number of cores;
* SSDs with millions of I/O per second;
* NICs with some 100 GB/s;

it is important to efficiently use them to scale linearly with evolution of the modern hardware. This can mean many things in practice. For instance, moving from one SSD to two should double the number of I/O's per second. Or doubling the number of CPU cores should double the amount of computation possible. Or even doubling the number of NICs should double the network throughput.

The simplest and most traditional way of achieving that goal is by employing threads. Place some shared data onto the heap; protect it with a lock, and then have all threads of execution acquire the lock only when accessing the data. This model has many great properties:

* It's easy to convert single-threaded programs to multi-threaded programs because there is no need to change the data model from the single-threaded version. Adding a lock around the data would suffice.

* The program is easy to develop and maintain. It is a synchronous, imperative list of statements from top to bottom. The scheduler of operating system can interrupt threads, allowing for efficient time-sharing of CPU resources.

Unfortunately, as the number of threads scales up, contention on the lock around the shared data does too.
Even if there a multiple threading running, just one of these threads will make progress at a time, leaving the rest doing nothing but wait. Every time a thread needs to give way to another thread, there is a context switch. Context switches are expensive, costing around five microseconds. That doesn’t sound expensive, but if we take into account that famed Linux Developer Jens Axboe just recently published results for his new io_uring kernel infrastructure with Storage I/O times below four microseconds, that means that we are now at a point where a context switch between threads is more expensive than an I/O operation!

To get rid of the overhead and achieve goal of scaling linearly with addition of hardware resources, the software's threads of execution must be independent from one another as much as possible. In practice, that means avoiding software locks and even atomic instructions.

Thread-per-core is an architecture that meets this goal. Instead of placing shared data in a global location that all threads access after acquiring a lock, thread-per-core will assign that data to a single thread, which governs a CPU processor and follows run-to-complete paradigm. When a thread wants to access  data of a different thread, it passes a message to the owning thread to perform the operation on its behalf.

This pattern has several advantages:
* No or minimal context switches
* cache friendly, including i-cache, d-cache and LLC
* NUMA friendly
* No lock contention

Thread-per-core architecture is the shared decision in the storage communities, both kernel solution(io-uring) and kernel-bypass counterpart, SPDK. [Research](https://atlarge-research.com/pdfs/2022-systor-apis.pdf) shows both of them can saturate millions of IOPS of NVMe SSD. Further, they [improve tail latencies of applications by up to 71%](https://helda.helsinki.fi//bitstream/handle/10138/313642/tpc_ancs19.pdf?sequence=1).


### Predictable Latency

Messaging, publishing in particular, usually resides at the critical path of application, thus is very sensitive to latency. As a general distributed stream storage, it important to have predictable latency such to meet SLA of various products on top of it.

#### Native Programming Language without GC
We are targeting latency of 2ms for P99, up to 1 GiB/s traffic for EC2 with a spec around 2C8G. Programming language with a garbage collector is not a good candidate. Thankfully, Rust offers satisfying performance, featuring itself with:
* Memory safety without garbage collection;
* Concurrency without data races;

#### Chasing Write
Chasing write is to cope with abnormal system jitters. Our system frontend can acknowledge application when a majority of replica manages to write data. For example, with a configuration of 2 acks out of 3 replica, it allows frontend client to ack the write request even if the third node suffers packet retransmission caused by temporary network issue. After network recovery, the slow node may chase up with the help of write retries.

#### Hedging Read

To reduce the read latency under dynamic environments, the client sends additional read requests to other nodes as backups before receiving the response of the previously sent read request.

#### Non-stop Write
Our system runs on commodity hardwares and they fail from time to time. To mitigate impacts of a node failure, frontend client seals the range and report successfully written data length to metadata manager and then create a new range to continue the unfinished data. New range are designed to placed on a new set of nodes, therefore, writes manage to fail over, experimentally within 20ms~50ms.

#### Hierarchy of Cache
To achieve maximum spatial locality, every component is armed with a cache. Frontend client caches hottest data in memory; Server nodes cache hot data in memory and warm data in SSDs; Frequently access OSS files are also cached in SSD.

#### Flatbuffers
It's true that our system is data-intensive; however, introduction of thread-per-core manages shift bottleneck from I/O to CPU. [Literature]((https://www.usenix.org/conference/fast23/presentation/li-qiang-deployed) shows that serialization and deserialization of RPCs on data path costs about 30% of CPU. Adopting flatbuffers avoids this overhead and results in 59% network throughput gain.

## Autonomous
Being autonomous means our system is capable of self-management and self-healing.

### Separation of Concern

#### Separation of Storage and Compute
Scale Independently

#### Separation of CP and AP

CP = Consistency + Partition

AP = Availability + Partition

Strong consistency while RTO tends to be zero
RPO = 0 and RTO -> 0


## Reference
1. [DFS Architecture Comparison](https://www.infoq.com/articles/dfs-architecture-comparison/)
2. [Glommio Design](https://www.datadoghq.com/blog/engineering/introducing-glommio/)
3. [The Impact of Thread-Per-Core Architecture on Application Tail Latency](https://helda.helsinki.fi//bitstream/handle/10138/313642/tpc_ancs19.pdf?sequence=1)
4. [More Than Capacity: Performance-oriented Evolution of Pangu in Alibaba](https://www.usenix.org/conference/fast23/presentation/li-qiang-deployed)
5. [SPDK: Message Passing and Concurrency Theory](https://spdk.io/doc/concurrency.html)
6. [The-5-minute-rule](https://www.allthingsdistributed.com/2012/08/the-5-minute-rule.html)
7. [Windows Azure Storage](https://www.cs.purdue.edu/homes/csjgwang/CloudNativeDB/AzureStorageSOSP11.pdf)
