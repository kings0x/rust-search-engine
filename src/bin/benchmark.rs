use anyhow::{Context, Result};
use search_engine::search::SearchIndex;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

fn main() -> Result<()> {
    let index_dir = env_path("SEARCH_INDEX_DIR", "./index_data");
    let query_file = env_path("BENCHMARK_QUERIES_FILE", "./benchmark_queries.txt");
    let requests = env_usize("BENCHMARK_REQUESTS", 100_000)?;
    let warmup = env_usize("BENCHMARK_WARMUP", 2_000)?;
    let limit = env_usize("BENCHMARK_LIMIT", 10)?;
    let default_concurrency = std::thread::available_parallelism().map_or(1, usize::from);
    let concurrency =
        env_usize("BENCHMARK_CONCURRENCY", default_concurrency)?.clamp(1, requests.max(1));

    if requests == 0 {
        anyhow::bail!("BENCHMARK_REQUESTS must be greater than zero");
    }

    let queries = Arc::new(read_queries(&query_file)?);
    let load_started = Instant::now();
    let index = Arc::new(
        SearchIndex::load(&index_dir)
            .with_context(|| format!("failed to load index from {}", index_dir.display()))?,
    );
    let load_elapsed = load_started.elapsed();
    let stats = index.stats();

    for request in 0..warmup {
        std::hint::black_box(index.search(&queries[request % queries.len()], limit));
    }

    let next_request = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let index = Arc::clone(&index);
        let queries = Arc::clone(&queries);
        let next_request = Arc::clone(&next_request);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let mut durations = Vec::with_capacity(requests / concurrency + 1);
            let mut returned_hits = 0_usize;
            barrier.wait();
            loop {
                let request = next_request.fetch_add(1, Ordering::Relaxed);
                if request >= requests {
                    break;
                }
                let started = Instant::now();
                let response = index.search(&queries[request % queries.len()], limit);
                durations.push(started.elapsed());
                returned_hits += response.hits.len();
            }
            (durations, returned_hits)
        }));
    }

    let benchmark_started = Instant::now();
    barrier.wait();
    let mut durations = Vec::with_capacity(requests);
    let mut returned_hits = 0_usize;
    for worker in workers {
        let (mut worker_durations, worker_hits) = worker.join().expect("benchmark worker panicked");
        durations.append(&mut worker_durations);
        returned_hits += worker_hits;
    }
    let wall_time = benchmark_started.elapsed();
    durations.sort_unstable();

    let qps = requests as f64 / wall_time.as_secs_f64();
    println!("Rust Search Engine benchmark (in-process, release build recommended)");
    println!("documents: {}", stats.documents);
    println!("terms: {}", stats.terms);
    println!("postings: {}", stats.postings);
    println!("index size: {} bytes", directory_size(&index_dir)?);
    println!("index load: {:.3} ms", milliseconds(load_elapsed));
    println!("queries: {}", queries.len());
    println!("warmup requests: {warmup}");
    println!("measured requests: {requests}");
    println!("concurrency: {concurrency}");
    println!("result limit: {limit}");
    println!("returned hits: {returned_hits}");
    println!("wall time: {:.3} s", wall_time.as_secs_f64());
    println!("throughput: {qps:.2} queries/s");
    println!(
        "p50 latency: {:.3} ms",
        milliseconds(percentile(&durations, 50))
    );
    println!(
        "p95 latency: {:.3} ms",
        milliseconds(percentile(&durations, 95))
    );
    println!(
        "p99 latency: {:.3} ms",
        milliseconds(percentile(&durations, 99))
    );
    println!(
        "max latency: {:.3} ms",
        milliseconds(*durations.last().unwrap())
    );
    println!(
        "host: {} {} ({} logical threads)",
        std::env::consts::OS,
        std::env::consts::ARCH,
        default_concurrency
    );
    Ok(())
}

fn read_queries(path: &Path) -> Result<Vec<String>> {
    let queries: Vec<String> = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read query file {}", path.display()))?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect();
    if queries.is_empty() {
        anyhow::bail!("query file {} contains no queries", path.display());
    }
    Ok(queries)
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn directory_size(path: &Path) -> Result<u64> {
    WalkDir::new(path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .try_fold(0_u64, |total, entry| Ok(total + entry.metadata()?.len()))
}

fn env_path(name: &str, default: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("{name} must be a positive integer"))
    })
}
