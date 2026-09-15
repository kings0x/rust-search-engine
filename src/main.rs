mod server;
use anyhow::{Context, Result};
use search_engine::{search::SearchIndex, storage};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let input = env_path("SEARCH_DOCUMENTS_DIR", "./some");
    let output = env_path("SEARCH_INDEX_DIR", "./index_data");
    let bind_address =
        std::env::var("SEARCH_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    if !input.is_dir() {
        anyhow::bail!(
            "document directory does not exist or is not a directory: {}",
            input.display()
        );
    }

    storage::ingest::build_index(&input, &output)
        .await
        .with_context(|| format!("failed to index {}", input.display()))?;
    let index = SearchIndex::load(&output)
        .with_context(|| format!("failed to load index from {}", output.display()))?;

    let stats = index.stats();
    println!(
        "loaded {} documents, {} terms, and {} postings",
        stats.documents, stats.terms, stats.postings
    );
    println!("search UI: http://{bind_address}");

    server::serve(index, &bind_address).await
}

fn env_path(name: &str, default: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}
