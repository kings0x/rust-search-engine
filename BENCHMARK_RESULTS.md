# Benchmark Results

## 24,000-article Wikipedia baseline

Measured on 16 September 2026 after commit `1bc6ff7`.

### Corpus and index

- Source: Hugging Face `wikimedia/wikipedia`, configuration `20231101.en`
- Selection: fixed-seed block sample distributed across 6,407,814 dataset rows
- Sample seed: `1592598564`
- Documents: 24,000
- Extracted corpus size: 69.66 MiB
- Index build time: 112.988 seconds
- Index size: 39,243,171 bytes (37.43 MiB)
- Vocabulary: 262,147 terms
- Postings: 4,592,105

Each selected dataset row is one article. The downloader skipped empty and dataset-viewer-truncated rows. The generated external corpus manifest preserves the exact dataset configuration, seed, row count, and license notice.

### Hardware

- CPU: Intel Core i5-10310U at 1.70 GHz
- CPU topology: 4 physical cores, 8 logical processors
- Memory: 15.7 GiB
- OS: Windows 11 Pro x86-64
- Build: Rust `--release`

### Workload

- Query set: 100 fixed mixed-topic queries in `benchmark_queries.txt`
- Warm-up: 5,000 requests per run
- Measured requests: 100,000 per run
- Concurrency: 8
- Result limit: top 10
- Scope: in-process `SearchIndex::search`; excludes HTTP, JSON serialization, and network time

### Repeated results

| Run | Throughput | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 11,591.29 queries/s | 0.322 ms | 2.647 ms | 5.375 ms | 20.112 ms |
| 2 | 9,206.15 queries/s | 0.334 ms | 3.536 ms | 8.290 ms | 96.520 ms |
| 3 | 9,862.24 queries/s | 0.320 ms | 1.964 ms | 9.447 ms | 140.145 ms |

The median-throughput run achieved **9,862.24 queries/s with 9.447 ms p99 latency**. Across all three runs, throughput ranged from 9,206.15 to 11,591.29 queries/s and p99 ranged from 5.375 to 9.447 ms.

These are local engine measurements for this hardware and workload, not guaranteed production or HTTP performance. Background operating-system activity affected tail latency, which is why every repeated run is shown instead of reporting only the best result.
