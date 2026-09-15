use std::path::{Path, PathBuf};

use anyhow::Result;
use search_engine::storage::ingest;

const MERGE_INTERVAL_SECS: u64 = 30;
const MERGE_THRESHOLD: usize = 10;

pub struct Server {
    output_dir: PathBuf,
}

impl Server {
    pub fn new(output_dir: &Path) -> Self {
        Self {
            output_dir: output_dir.to_path_buf(),
        }
    }

    pub async fn start(self) -> Result<()> {
        let output_dir = self.output_dir.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(MERGE_INTERVAL_SECS)).await;
                if let Err(e) = try_merge(&output_dir).await {
                    eprintln!("background merge error: {e}");
                }
            }
        });

        // Placeholder: in the future, this is where the query listener runs
        // e.g. an HTTP or TCP server accepting search queries
        // For now, keep the process alive
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }
}

async fn try_merge(output_dir: &Path) -> Result<()> {
    let segments = ingest::read_manifest(output_dir)?;
    if segments.len() <= MERGE_THRESHOLD {
        return Ok(());
    }

    // Pick smallest segments by doc_count, merge half of them
    let mut to_merge = segments.to_vec();
    to_merge.sort_by_key(|s| s.doc_count);
    let merge_count = to_merge.len() / 2;
    let to_merge: Vec<_> = to_merge.drain(..merge_count).collect();

    let new_seg_id = segments
        .iter()
        .map(|segment| segment.seg_id)
        .max()
        .unwrap_or(0)
        + 1;

    let new_seg = ingest::merge_segments_background(output_dir, &to_merge, new_seg_id)?;

    // Build new manifest: exclude old segments, add new one
    let merged_ids: std::collections::HashSet<_> = to_merge.iter().map(|s| s.seg_id).collect();
    let mut new_manifest: Vec<_> = segments
        .iter()
        .filter(|s| !merged_ids.contains(&s.seg_id))
        .cloned()
        .collect();
    new_manifest.push(new_seg);
    new_manifest.sort_by_key(|s| s.seg_id);

    ingest::write_manifest_atomic(&new_manifest, output_dir)?;

    // Delete old segment files
    for seg in &to_merge {
        let prefix = output_dir.join(format!("seg_{}", seg.seg_id));
        let _ = std::fs::remove_file(prefix.with_extension("postings.txt"));
        let _ = std::fs::remove_file(prefix.with_extension("offsets.txt"));
        let _ = std::fs::remove_file(prefix.with_extension("alphas.txt"));
        let _ = std::fs::remove_file(prefix.with_extension("docs.txt"));
    }

    eprintln!(
        "merged {} segments ({}-{}) into seg_{}",
        to_merge.len(),
        to_merge.first().unwrap().seg_id,
        to_merge.last().unwrap().seg_id,
        new_seg_id
    );
    Ok(())
}
