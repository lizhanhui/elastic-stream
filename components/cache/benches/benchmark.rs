use cache::SizedValue;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

static GB_TO_BYTES: u64 = 1024 * 1024 * 1024;

static CACHE_SIZE: [u64; 4] = [1, 5, 10, 50];

#[derive(Clone)]
struct CacheItem4K {}

impl SizedValue for CacheItem4K {
    fn size(&self) -> u64 {
        4096
    }
}

fn cache_4k(c: &mut Criterion) {
    let mut group = c.benchmark_group("Cache 4K Item Insert");

    for base in CACHE_SIZE {
        let mut rng = StdRng::seed_from_u64(0x1234abcd);
        let cache_size = base * GB_TO_BYTES;
        let mut cache = cache::HierarchicalCache::<u32, CacheItem4K>::new(cache_size, 80);
        for _i in 0..cache_size / 4096 {
            let key = rng.gen::<u32>();
            cache.push_active(key, CacheItem4K {});
        }
        group.bench_function(BenchmarkId::from_parameter(base), |b| {
            b.iter(|| {
                let key = rng.gen::<u32>();
                cache.push_active(key, CacheItem4K {});
            })
        });
    }
}

#[derive(Clone)]
struct CacheItem4M {}

impl SizedValue for CacheItem4M {
    fn size(&self) -> u64 {
        4096 * 1024
    }
}

fn cache_4m(c: &mut Criterion) {
    let mut group = c.benchmark_group("Cache 4M Item Insert");

    for base in CACHE_SIZE {
        let mut rng = StdRng::seed_from_u64(0x1234abcd);
        let cache_size = base * GB_TO_BYTES;
        let mut cache = cache::HierarchicalCache::<u32, CacheItem4M>::new(cache_size, 80);
        for _i in 0..cache_size / (4096 * 1024) {
            let key = rng.gen::<u32>();
            cache.push_active(key, CacheItem4M {});
        }
        group.bench_function(BenchmarkId::from_parameter(base), |b| {
            b.iter(|| {
                let key = rng.gen::<u32>();
                cache.push_active(key, CacheItem4M {});
            })
        });
    }
}

criterion_group!(benches, cache_4k, cache_4m);
criterion_main!(benches);
