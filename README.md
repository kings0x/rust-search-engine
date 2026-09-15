# Rust Search Engine

A learning-focused full-text search engine written in Rust. It crawls a directory of UTF-8 text documents, builds a persistent positional inverted index, and serves BM25-ranked results through a small web interface and JSON API.

## What it implements

- Concurrent, bounded-memory document ingestion with streaming for large files
- Case normalization, tokenization, and English stemming
- Positional inverted indexes split into persistent segments
- Delta-encoded document IDs and positions with variable-byte compression
- BM25 relevance ranking (`k1 = 1.2`, `b = 0.75`)
- Trigram spelling suggestions for out-of-vocabulary query terms
- Browser search interface and JSON endpoints
- A concurrent, in-process latency and throughput benchmark
- Backward-compatible reads for the original text posting-list format

## How the search path works

```text
documents -> analyze -> positional postings -> compressed segments on disk
                                                       |
query -> analyze -> posting-list lookup -> BM25 score -> top results
                    |
                    +-> trigram lookup for unknown terms
```

Each input file is one searchable document. The index stores its path and analyzed token count. A posting records the document ID, term frequency, and term positions.

BM25 scores each matching query term using its rarity in the collection, its frequency in the document, and the document length relative to the collection average. A query uses OR semantics: matching more query terms usually improves a document's score.

## Run it

Install a current Rust toolchain, then point the server at a directory containing UTF-8 text files.

PowerShell:

```powershell
$env:SEARCH_DOCUMENTS_DIR = "C:\path\to\documents"
$env:SEARCH_INDEX_DIR = ".\index_data"
$env:SEARCH_BIND = "127.0.0.1:3000"
cargo run --release --bin search-engine
```

Bash:

```bash
SEARCH_DOCUMENTS_DIR=/path/to/documents \
SEARCH_INDEX_DIR=./index_data \
SEARCH_BIND=127.0.0.1:3000 \
cargo run --release --bin search-engine
```

Open `http://127.0.0.1:3000`. Startup reports how many new documents were indexed and how long indexing took. Existing indexed paths are retained and skipped on later runs.

The default input is `./some`, the default index is `./index_data`, and the default bind address is `127.0.0.1:3000`.

## HTTP API

- `GET /` — browser interface
- `GET /api/search?q=rust+search&limit=10` — ranked results and spelling suggestions
- `GET /api/stats` — document, vocabulary, posting, and average-length counts
- `GET /health` — health check

The result `took_micros` measures search-engine work inside the request. It does not include browser, network, or JSON transfer time.

## Index files

Every segment has these files:

- `seg_N.postings.bin` — delta + variable-byte encoded posting lists
- `seg_N.lexicon.tsv` — term, byte offset, byte length, document frequency, and collection frequency
- `seg_N.docs.txt` — document ID, analyzed length, and source path
- `manifest.txt` — active segment metadata

Generated indexes are ignored by Git. To build a clean experimental index without touching an existing one, choose a new `SEARCH_INDEX_DIR`.

## Benchmarking

Build and query in release mode. Put representative queries in `benchmark_queries.txt`, one per line, then run:

```powershell
$env:SEARCH_INDEX_DIR = ".\index_data_wikipedia"
$env:BENCHMARK_QUERIES_FILE = ".\benchmark_queries.txt"
$env:BENCHMARK_REQUESTS = "100000"
$env:BENCHMARK_WARMUP = "2000"
$env:BENCHMARK_CONCURRENCY = "8"
$env:BENCHMARK_LIMIT = "10"
cargo run --release --bin benchmark
```

The harness reports corpus/index size, load time, workload settings, QPS, and p50/p95/p99/max latency. It calls `SearchIndex::search` directly, so it measures the ranking engine rather than HTTP, JSON serialization, or network overhead. Use an HTTP load generator separately if the claim is about end-to-end API performance.

For a Wikipedia result, prepare at least 24,000 articles as individual UTF-8 files, use a fresh index directory, and preserve the exact corpus snapshot and query list. Do not describe a tiny or synthetic corpus as Wikipedia. A defensible result looks like:

> On `<CPU/RAM/OS>`, in-process top-10 BM25 search over `<document count>` Wikipedia articles and `<query count>` representative queries achieved `<QPS>` at concurrency `<N>`, with `<p99>` ms p99 latency across `<request count>` measured requests after `<warmup count>` warm-up requests.

“Indexed 24K documents” describes corpus size; p99 and QPS describe query performance. Index build duration and indexing throughput should be reported separately. High QPS and low p99 are useful metrics, but only with fixed hardware, concurrency, query distribution, result limit, warm-up, and in-process-versus-HTTP scope.

## Verify changes

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Current limitations

- Input files must be valid UTF-8, and one file represents one document.
- Incremental indexing adds previously unseen paths; it does not yet detect edited or deleted source files.
- The in-memory query index favors simple, fast experiments over indexes larger than available RAM.
- English stemming is always enabled.
