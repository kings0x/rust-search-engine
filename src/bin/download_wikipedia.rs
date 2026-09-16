use anyhow::{Context, Result};
use reqwest::Client;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DATASET: &str = "wikimedia/wikipedia";
const CONFIG: &str = "20231101.en";
const SPLIT: &str = "train";
const DATASET_API: &str = "https://datasets-server.huggingface.co/rows";
const BATCH_SIZE: usize = 100;
const DEFAULT_ARTICLE_COUNT: usize = 24_000;
const DEFAULT_SEED: u64 = 0x5EED_2024;
const MAX_ATTEMPTS: usize = 8;
const REQUEST_DELAY: Duration = Duration::from_millis(1_500);

#[derive(Debug, Deserialize)]
struct RowsResponse {
    rows: Vec<DatasetRow>,
    num_rows_total: usize,
}

#[derive(Debug, Deserialize)]
struct DatasetRow {
    row: Article,
    truncated_cells: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Article {
    id: String,
    url: String,
    title: String,
    text: String,
}

#[derive(Debug, Serialize)]
struct CorpusManifest {
    dataset: &'static str,
    config: &'static str,
    split: &'static str,
    source_api: &'static str,
    article_count: usize,
    dataset_rows: usize,
    sample_seed: u64,
    batch_size: usize,
    generated_unix_seconds: u64,
    license: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let output_dir = env_path("WIKIPEDIA_OUTPUT_DIR", "./corpora/wikipedia-24k");
    let target_count = env_usize("WIKIPEDIA_ARTICLES", DEFAULT_ARTICLE_COUNT)?;
    let seed = env_u64("WIKIPEDIA_SEED", DEFAULT_SEED)?;
    if target_count == 0 {
        anyhow::bail!("WIKIPEDIA_ARTICLES must be greater than zero");
    }

    let documents_dir = output_dir.join("documents");
    std::fs::create_dir_all(&documents_dir)?;
    let client = Client::builder()
        .user_agent("rust-search-engine-corpus-builder/0.1")
        .timeout(Duration::from_secs(90))
        .build()?;

    let dataset_rows = fetch_rows(&client, 0, 1).await?.num_rows_total;
    if target_count > dataset_rows {
        anyhow::bail!(
            "requested {target_count} articles, but the dataset only reports {dataset_rows} rows"
        );
    }

    let mut existing_ids = existing_article_ids(&documents_dir)?;
    let mut accepted = existing_ids.len().min(target_count);
    let mut sampled_blocks = HashSet::new();
    let mut random_state = seed;
    let block_count = dataset_rows.div_ceil(BATCH_SIZE);

    println!("dataset rows: {dataset_rows}");
    println!("target articles: {target_count}");
    println!("sample seed: {seed}");
    println!("output: {}", documents_dir.display());
    if accepted > 0 {
        println!("resuming with {accepted} existing articles");
    }

    while accepted < target_count {
        let block = next_random(&mut random_state) as usize % block_count;
        if !sampled_blocks.insert(block) {
            continue;
        }
        let offset = block * BATCH_SIZE;
        let length = BATCH_SIZE.min(dataset_rows - offset);
        let response = fetch_rows(&client, offset, length).await?;

        for dataset_row in response.rows {
            if accepted >= target_count {
                break;
            }
            if !dataset_row.truncated_cells.is_empty()
                || dataset_row.row.text.trim().is_empty()
                || existing_ids.contains(&dataset_row.row.id)
            {
                continue;
            }

            write_article(&documents_dir, &dataset_row.row)?;
            existing_ids.insert(dataset_row.row.id);
            accepted += 1;
            if accepted % 500 == 0 || accepted == target_count {
                println!("downloaded {accepted}/{target_count} articles");
            }
        }
        tokio::time::sleep(REQUEST_DELAY).await;
    }

    let manifest = CorpusManifest {
        dataset: DATASET,
        config: CONFIG,
        split: SPLIT,
        source_api: DATASET_API,
        article_count: accepted,
        dataset_rows,
        sample_seed: seed,
        batch_size: BATCH_SIZE,
        generated_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        license: "CC BY-SA 3.0 and GFDL; see the source dataset card",
    };
    std::fs::write(
        output_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!("corpus complete: {} articles", manifest.article_count);
    Ok(())
}

async fn fetch_rows(client: &Client, offset: usize, length: usize) -> Result<RowsResponse> {
    let mut last_error = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let result = client
            .get(DATASET_API)
            .query(&[
                ("dataset", DATASET),
                ("config", CONFIG),
                ("split", SPLIT),
                ("offset", &offset.to_string()),
                ("length", &length.to_string()),
            ])
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);

        let mut rate_limited = false;
        match result {
            Ok(response) => match response.json::<RowsResponse>().await {
                Ok(rows) => return Ok(rows),
                Err(error) => last_error = Some(anyhow::Error::new(error)),
            },
            Err(error) => {
                rate_limited = error.status() == Some(StatusCode::TOO_MANY_REQUESTS);
                last_error = Some(anyhow::Error::new(error));
            }
        }

        if attempt < MAX_ATTEMPTS {
            let delay = if rate_limited {
                Duration::from_secs(60)
            } else {
                Duration::from_secs((1 << (attempt - 1)).min(60))
            };
            eprintln!(
                "request at offset {offset} failed (attempt {attempt}/{MAX_ATTEMPTS}); retrying in {}s",
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;
        }
    }

    Err(last_error.unwrap()).with_context(|| {
        format!(
            "failed to download Wikipedia rows at offset {offset} after {MAX_ATTEMPTS} attempts"
        )
    })
}

fn write_article(documents_dir: &Path, article: &Article) -> Result<()> {
    let safe_id: String = article
        .id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .collect();
    if safe_id.is_empty() {
        anyhow::bail!("article has an invalid empty ID: {}", article.id);
    }

    let final_path = documents_dir.join(format!("{safe_id}.txt"));
    let temporary_path = documents_dir.join(format!(".{safe_id}.downloading"));
    let contents = format!("{}\n{}\n\n{}\n", article.title, article.url, article.text);
    std::fs::write(&temporary_path, contents)
        .with_context(|| format!("failed to write {}", temporary_path.display()))?;
    std::fs::rename(&temporary_path, &final_path)
        .with_context(|| format!("failed to finalize {}", final_path.display()))?;
    Ok(())
}

fn existing_article_ids(documents_dir: &Path) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for entry in std::fs::read_dir(documents_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "txt")
            && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            ids.insert(stem.to_string());
        }
    }
    Ok(ids)
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
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

fn env_u64(name: &str, default: u64) -> Result<u64> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer"))
    })
}
