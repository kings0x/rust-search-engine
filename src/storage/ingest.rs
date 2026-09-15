use anyhow::Result;
use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use walkdir::WalkDir;

const MAX_CONCURRENT_TASKS: usize = 128;
const SPAWN_BACKPRESSURE: usize = MAX_CONCURRENT_TASKS * 4;

const MAX_MEMORY_TERMS: usize = 500_000;
const MEMORY_CHECK_INTERVAL: usize = 50;

const STREAM_THRESHOLD: u64 = 64 * 1024 * 1024;
const STREAM_CHUNK_SIZE: usize = 256 * 1024;
const STREAM_PERMIT_COST: u32 = 1;
const MAX_MEMORY_MB: u32 = 2048;

#[derive(Debug, Clone)]
pub struct Posting {
    pub doc_id: u32,
    pub frequency: u32,
    pub positions: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct DocMeta {
    pub path: String,
    pub length: u32,
}

type PerDocPostings = HashMap<String, (u32, Vec<u32>)>;

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub seg_id: u32,
    pub start_doc_id: u32,
    pub doc_count: u32,
}

fn tokenize_text(text: &str) -> PerDocPostings {
    let re = Regex::new(r"[^a-zA-Z0-9\s]+").unwrap();
    let stemmer = Stemmer::create(Algorithm::English);
    let cleaned = re.replace_all(text, " ");
    let mut postings: PerDocPostings = HashMap::new();

    for (pos, word) in cleaned.split_whitespace().enumerate() {
        let stemmed = stemmer.stem(word).to_string();
        let entry = postings.entry(stemmed).or_insert((0, Vec::new()));
        entry.0 += 1;
        entry.1.push(pos as u32);
    }

    postings
}

async fn process_file_bulk(path: &Path) -> Result<(DocMeta, PerDocPostings)> {
    let contents = tokio::fs::read_to_string(path).await?;
    let word_count = contents.split_whitespace().count() as u32;
    let postings = tokenize_text(&contents);
    let meta = DocMeta {
        path: path.to_string_lossy().to_string(),
        length: word_count,
    };
    Ok((meta, postings))
}

async fn process_file_stream(path: &Path) -> Result<(DocMeta, PerDocPostings)> {
    let file = tokio::fs::File::open(path).await?;
    let reader = BufReader::with_capacity(STREAM_CHUNK_SIZE, file);
    let mut lines = reader.lines();

    let mut postings: PerDocPostings = HashMap::new();
    let mut global_pos: u32 = 0;
    let re = Regex::new(r"[^a-zA-Z0-9\s]+").unwrap();
    let stemmer = Stemmer::create(Algorithm::English);

    while let Some(line) = lines.next_line().await? {
        let cleaned = re.replace_all(&line, " ");
        for word in cleaned.split_whitespace() {
            let stemmed = stemmer.stem(word).to_string();
            let entry = postings.entry(stemmed).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(global_pos);
            global_pos += 1;
        }
    }

    let meta = DocMeta {
        path: path.to_string_lossy().to_string(),
        length: global_pos,
    };
    Ok((meta, postings))
}

// ── Segment writer ──

fn write_segment_to_disk(
    seg_id: u32,
    docs: &[DocMeta],
    vocabulary: &HashMap<String, Vec<Posting>>,
    output_dir: &Path,
) -> Result<()> {
    use std::io::Write;

    let prefix = output_dir.join(format!("seg_{}", seg_id));

    let mut sorted: Vec<&String> = vocabulary.keys().collect();
    sorted.sort();

    let mut pf = std::fs::File::create(prefix.with_extension("postings.txt"))?;
    let mut of = std::fs::File::create(prefix.with_extension("offsets.txt"))?;
    let mut af = std::fs::File::create(prefix.with_extension("alphas.txt"))?;

    for term in &sorted {
        let postings = &vocabulary[*term];

        let mut line = String::new();
        line.push_str(term);
        line.push('|');
        for (i, p) in postings.iter().enumerate() {
            if i > 0 {
                line.push(';');
            }
            line.push_str(&format!("{},{}", p.doc_id, p.frequency));
            for pos in &p.positions {
                line.push(',');
                line.push_str(&pos.to_string());
            }
        }
        line.push('\n');

        let offset = pf.metadata()?.len();
        writeln!(of, "{}", offset)?;
        let doc_freq = postings.len();
        let total_freq: u32 = postings.iter().map(|p| p.frequency).sum();
        writeln!(af, "{}:{}:{}", term, doc_freq, total_freq)?;
        pf.write_all(line.as_bytes())?;
    }

    let mut df = std::fs::File::create(prefix.with_extension("docs.txt"))?;
    for doc in docs {
        writeln!(df, "{}|{}", doc.path, doc.length)?;
    }

    Ok(())
}

// ── Manifest ──

pub fn read_manifest(output_dir: &Path) -> Result<Vec<SegmentMeta>> {
    let path = output_dir.join("manifest.txt");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(&path)?;
    let mut segments = Vec::new();
    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 {
            segments.push(SegmentMeta {
                seg_id: parts[0].parse()?,
                start_doc_id: parts[1].parse()?,
                doc_count: parts[2].parse()?,
            });
        }
    }
    Ok(segments)
}

fn read_indexed_paths(output_dir: &Path) -> Result<Vec<String>> {
    let segments = read_manifest(output_dir)?;
    let mut paths = Vec::new();
    for seg in &segments {
        let path = output_dir.join(format!("seg_{}.docs.txt", seg.seg_id));
        if let Ok(content) = std::fs::read_to_string(&path) {
            for line in content.lines() {
                if let Some(path_part) = line.split('|').next() {
                    paths.push(path_part.to_string());
                }
            }
        }
    }
    Ok(paths)
}

pub fn write_manifest(segments: &[SegmentMeta], output_dir: &Path) -> Result<()> {
    use std::io::Write;
    let path = output_dir.join("manifest.txt");
    let mut file = std::fs::File::create(&path)?;
    for seg in segments {
        writeln!(
            file,
            "{} {} {}",
            seg.seg_id, seg.start_doc_id, seg.doc_count
        )?;
    }
    Ok(())
}

pub fn write_manifest_atomic(segments: &[SegmentMeta], output_dir: &Path) -> Result<()> {
    use std::io::Write;
    let tmp = output_dir.join("manifest.tmp");
    let final_path = output_dir.join("manifest.txt");
    let mut file = std::fs::File::create(&tmp)?;
    for seg in segments {
        writeln!(
            file,
            "{} {} {}",
            seg.seg_id, seg.start_doc_id, seg.doc_count
        )?;
    }
    drop(file);
    std::fs::rename(&tmp, &final_path)?;
    Ok(())
}

// ── Memory-managed builder ──

struct IndexBuilder {
    docs: Vec<DocMeta>,
    vocabulary: HashMap<String, Vec<Posting>>,
    next_doc_id: u32,
    next_seg_id: u32,
    docs_since_last_check: usize,
    output_dir: PathBuf,
    new_segments: Vec<SegmentMeta>,
}

impl IndexBuilder {
    fn new(output_dir: &Path, existing: &[SegmentMeta]) -> Self {
        let next_doc_id = existing
            .last()
            .map(|s| s.start_doc_id + s.doc_count)
            .unwrap_or(0);
        let next_seg_id = existing.last().map(|s| s.seg_id + 1).unwrap_or(0);

        Self {
            docs: Vec::new(),
            vocabulary: HashMap::new(),
            next_doc_id,
            next_seg_id,
            docs_since_last_check: 0,
            output_dir: output_dir.to_path_buf(),
            new_segments: Vec::new(),
        }
    }

    fn merge_document(&mut self, meta: DocMeta, postings: PerDocPostings) -> Result<()> {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;
        self.docs.push(meta);

        for (term, (freq, positions)) in postings {
            self.vocabulary.entry(term).or_default().push(Posting {
                doc_id,
                frequency: freq,
                positions,
            });
        }

        self.docs_since_last_check += 1;
        if self.docs_since_last_check >= MEMORY_CHECK_INTERVAL
            && self.vocabulary.len() >= MAX_MEMORY_TERMS
        {
            self.flush_segment()?;
        }
        Ok(())
    }

    fn flush_segment(&mut self) -> Result<()> {
        if self.vocabulary.is_empty() {
            return Ok(());
        }

        let seg_id = self.next_seg_id;
        let doc_count = self.docs.len() as u32;
        let start_doc_id = self.next_doc_id - doc_count;

        write_segment_to_disk(seg_id, &self.docs, &self.vocabulary, &self.output_dir)?;

        self.new_segments.push(SegmentMeta {
            seg_id,
            start_doc_id,
            doc_count,
        });

        self.next_seg_id += 1;
        self.vocabulary.clear();
        self.docs.clear();
        self.docs_since_last_check = 0;
        Ok(())
    }

    fn finalize(mut self, existing: Vec<SegmentMeta>) -> Result<Vec<SegmentMeta>> {
        self.flush_segment()?;
        let mut all = existing;
        all.extend(self.new_segments);
        Ok(all)
    }
}

pub async fn build_index(root: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let existing = read_manifest(output_dir)?;
    eprintln!(
        "indexing {:?} -> {:?} ({} existing segments)",
        root,
        output_dir,
        existing.len()
    );

    let indexed = read_indexed_paths(output_dir)?;
    let indexed_set: std::collections::HashSet<String> = indexed.into_iter().collect();

    let mem_sem = Arc::new(Semaphore::new(MAX_MEMORY_MB as usize));
    let task_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));
    let mut tasks: JoinSet<Result<(DocMeta, PerDocPostings)>> = JoinSet::new();
    let builder = std::sync::Mutex::new(IndexBuilder::new(output_dir, &existing));

    let walker = WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file());

    for entry in walker {
        let path_str = entry.path().to_string_lossy().to_string();
        if indexed_set.contains(&path_str) {
            eprintln!("skipping already indexed: {}", path_str);
            continue;
        }
        let path = entry.path().to_path_buf();
        let size = match entry.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                eprintln!("skipping {}: metadata error: {e}", path.display());
                continue;
            }
        };

        let mem_sem = mem_sem.clone();
        let task_sem = task_sem.clone();

        tasks.spawn(async move {
            let _task_permit = task_sem.acquire().await?;

            if size >= STREAM_THRESHOLD {
                let _mem = mem_sem.acquire_many(STREAM_PERMIT_COST).await?;
                process_file_stream(&path)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
            } else {
                let permits = ((size / (1024 * 1024)) as u32).max(1).min(MAX_MEMORY_MB);
                let _mem = mem_sem.acquire_many(permits).await?;
                process_file_bulk(&path)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
            }
        });

        while tasks.len() >= SPAWN_BACKPRESSURE {
            if let Some(res) = tasks.join_next().await {
                ingest_result(res, &builder)?;
            }
        }
    }

    while let Some(res) = tasks.join_next().await {
        ingest_result(res, &builder)?;
    }

    let all_segments = builder.into_inner().unwrap().finalize(existing)?;
    write_manifest(&all_segments, output_dir)?;
    eprintln!("done — {} segments", all_segments.len());
    Ok(())
}

struct SegReader {
    reader: std::io::BufReader<std::fs::File>,
    current_line: Option<String>,
}

pub fn merge_segments_background(
    output_dir: &Path,
    segments: &[SegmentMeta],
    new_seg_id: u32,
) -> Result<SegmentMeta> {
    use std::io::{BufRead, Write};

    // Collect all docs from input segments
    let mut all_docs: Vec<DocMeta> = Vec::new();
    for seg in segments {
        let path = output_dir.join(format!("seg_{}.docs.txt", seg.seg_id));
        let content = std::fs::read_to_string(&path)?;
        for line in content.lines() {
            if let Some((path_part, len_part)) = line.split_once('|') {
                all_docs.push(DocMeta {
                    path: path_part.to_string(),
                    length: len_part.parse()?,
                });
            }
        }
    }

    // Open all input postings files
    let mut seg_readers: Vec<SegReader> = Vec::new();
    for seg in segments {
        let path = output_dir.join(format!("seg_{}.postings.txt", seg.seg_id));
        let file = std::fs::File::open(&path)?;
        let mut reader = std::io::BufReader::new(file);
        let mut buf = String::new();
        let current_line = if reader.read_line(&mut buf)? > 0 {
            if buf.ends_with('\n') {
                buf.pop();
            }
            Some(buf)
        } else {
            None
        };
        seg_readers.push(SegReader {
            reader,
            current_line,
        });
    }

    // Open output files for the merged segment
    let prefix = output_dir.join(format!("seg_{}", new_seg_id));
    let mut out_pf = std::fs::File::create(prefix.with_extension("postings.txt"))?;
    let mut out_of = std::fs::File::create(prefix.with_extension("offsets.txt"))?;
    let mut out_af = std::fs::File::create(prefix.with_extension("alphas.txt"))?;

    loop {
        // Find the smallest term across all active readers
        let min_term = seg_readers
            .iter()
            .filter_map(|sr| sr.current_line.as_ref())
            .map(|line| line.split('|').next().unwrap())
            .min()
            .map(|s| s.to_string());

        let Some(ref min_term) = min_term else { break };

        let mut combined = min_term.clone();
        combined.push('|');
        let mut first_posting = true;
        let mut doc_freq: u32 = 0;
        let mut total_freq: u32 = 0;

        for sr in &mut seg_readers {
            let Some(ref line) = sr.current_line else {
                continue;
            };
            if line.split('|').next().unwrap() != min_term {
                continue;
            }

            if let Some(right) = line.split('|').nth(1) {
                for posting_str in right.split(';') {
                    if !first_posting {
                        combined.push(';');
                    }
                    first_posting = false;
                    combined.push_str(posting_str);

                    let parts: Vec<&str> = posting_str.split(',').collect();
                    if parts.len() >= 2 {
                        doc_freq += 1;
                        total_freq += parts[1].parse::<u32>().unwrap_or(0);
                    }
                }
            }

            // Advance this reader
            let mut buf = String::new();
            sr.current_line = if sr.reader.read_line(&mut buf)? > 0 {
                if buf.ends_with('\n') {
                    buf.pop();
                }
                Some(buf)
            } else {
                None
            };
        }

        combined.push('\n');

        let offset = out_pf.metadata()?.len();
        writeln!(out_of, "{}", offset)?;
        writeln!(out_af, "{}:{}:{}", min_term, doc_freq, total_freq)?;
        out_pf.write_all(combined.as_bytes())?;
    }

    let mut out_df = std::fs::File::create(prefix.with_extension("docs.txt"))?;
    for doc in &all_docs {
        writeln!(out_df, "{}|{}", doc.path, doc.length)?;
    }

    let start_doc_id = segments.first().unwrap().start_doc_id;
    let doc_count: u32 = segments.iter().map(|s| s.doc_count).sum();

    Ok(SegmentMeta {
        seg_id: new_seg_id,
        start_doc_id,
        doc_count,
    })
}

fn ingest_result(
    res: std::result::Result<Result<(DocMeta, PerDocPostings)>, tokio::task::JoinError>,
    builder: &std::sync::Mutex<IndexBuilder>,
) -> Result<()> {
    match res {
        Ok(Ok((meta, postings))) => {
            builder.lock().unwrap().merge_document(meta, postings)?;
        }
        Ok(Err(e)) => eprintln!("task error: {e}"),
        Err(e) => eprintln!("task panicked: {e}"),
    }
    Ok(())
}
