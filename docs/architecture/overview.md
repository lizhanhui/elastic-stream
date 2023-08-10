## Cloud Native Design

As technology advances and data continues to explode, traditional messaging and streaming platforms have exhibited their limitations in terms of infrastructure cost and operational management. To address these challenges and make most of technology innovations, now is the time to redesign streaming platform, to meet following desirable expectations:

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

Fundamentals have changed:
* SSD serves millions of IOPS while HDD typically have 200 IOPS;
* Sequential throughput, both read and write, is only 2x of their random counterparts for SSD; HDD, however, is generally 100x;
* High queue depth(some 32) is required to fully utilize its I/O capabilities;


### Object Storage
1. Unlimited Storage Capacity
2. Affordable via EC
3. SLA(Scalable + Availability + Reliability)

### Storage Advancements in NVMe SSD
1. Millions of IOPS
2. Thread-Per-Core(Run-to-Complete) architecture shifts bottleneck of IO-intensive workload from IO to CPU.
3. Scale with CPU processors
   Glommio, SPDK, io-uring, [monoio](https://github.com/bytedance/monoio), [Seastar](http://seastar.io/)

## Separation of Concern

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
