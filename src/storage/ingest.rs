use crate::analysis::analyze;
use crate::storage::codec::{decode_postings, encode_postings};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub doc_id: u32,
    pub frequency: u32,
    pub positions: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocMeta {
    pub doc_id: u32,
    pub path: String,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub seg_id: u32,
    pub start_doc_id: u32,
    pub doc_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBuildReport {
    pub indexed_documents: usize,
    pub previously_indexed_documents: usize,
    pub segments_written: usize,
    pub elapsed_millis: u128,
}

type PerDocPostings = HashMap<String, (u32, Vec<u32>)>;
type ProcessedDocument = (String, u32, PerDocPostings);

fn postings_from_terms(terms: &[String], starting_position: u32) -> PerDocPostings {
    let mut postings = HashMap::new();
    for (offset, term) in terms.iter().enumerate() {
        let entry = postings.entry(term.clone()).or_insert((0, Vec::new()));
        entry.0 += 1;
        entry.1.push(starting_position + offset as u32);
    }
    postings
}

async fn process_file_bulk(path: &Path) -> Result<ProcessedDocument> {
    let contents = tokio::fs::read_to_string(path).await?;
    let terms = analyze(&contents);
    let length = terms.len() as u32;
    let postings = postings_from_terms(&terms, 0);
    Ok((path.to_string_lossy().to_string(), length, postings))
}

async fn process_file_stream(path: &Path) -> Result<ProcessedDocument> {
    let file = tokio::fs::File::open(path).await?;
    let reader = BufReader::with_capacity(STREAM_CHUNK_SIZE, file);
    let mut lines = reader.lines();
    let mut postings: PerDocPostings = HashMap::new();
    let mut global_position = 0_u32;

    while let Some(line) = lines.next_line().await? {
        let terms = analyze(&line);
        for (term, (frequency, mut positions)) in postings_from_terms(&terms, global_position) {
            let entry = postings.entry(term).or_insert((0, Vec::new()));
            entry.0 += frequency;
            entry.1.append(&mut positions);
        }
        global_position += terms.len() as u32;
    }

    Ok((
        path.to_string_lossy().to_string(),
        global_position,
        postings,
    ))
}

fn write_segment_to_disk(
    seg_id: u32,
    docs: &[DocMeta],
    vocabulary: &HashMap<String, Vec<Posting>>,
    output_dir: &Path,
) -> Result<()> {
    use std::io::Write;

    let prefix = output_dir.join(format!("seg_{seg_id}"));
    let mut sorted_terms: Vec<&String> = vocabulary.keys().collect();
    sorted_terms.sort();

    let mut postings_file = std::fs::File::create(prefix.with_extension("postings.bin"))?;
    let mut lexicon_file = std::fs::File::create(prefix.with_extension("lexicon.tsv"))?;

    for term in sorted_terms {
        let mut postings = vocabulary[term].clone();
        postings.sort_by_key(|posting| posting.doc_id);

        let offset = postings_file.metadata()?.len();
        let encoded = encode_postings(&postings);
        postings_file.write_all(&encoded)?;
        let total_frequency: u32 = postings.iter().map(|posting| posting.frequency).sum();
        writeln!(
            lexicon_file,
            "{}\t{}\t{}\t{}\t{}",
            term,
            offset,
            encoded.len(),
            postings.len(),
            total_frequency
        )?;
    }

    let mut docs_file = std::fs::File::create(prefix.with_extension("docs.txt"))?;
    let mut sorted_docs = docs.to_vec();
    sorted_docs.sort_by_key(|document| document.doc_id);
    for document in sorted_docs {
        writeln!(
            docs_file,
            "{}\t{}\t{}",
            document.doc_id, document.length, document.path
        )?;
    }

    Ok(())
}

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

pub fn read_segment_docs(output_dir: &Path, segment: &SegmentMeta) -> Result<Vec<DocMeta>> {
    let path = output_dir.join(format!("seg_{}.docs.txt", segment.seg_id));
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let mut documents = Vec::new();

    for (line_index, line) in content.lines().enumerate() {
        let tab_parts: Vec<&str> = line.splitn(3, '\t').collect();
        if tab_parts.len() == 3 {
            documents.push(DocMeta {
                doc_id: tab_parts[0].parse()?,
                length: tab_parts[1].parse()?,
                path: tab_parts[2].to_string(),
            });
        } else if let Some((path, length)) = line.rsplit_once('|') {
            documents.push(DocMeta {
                doc_id: segment.start_doc_id + line_index as u32,
                length: length.parse()?,
                path: path.to_string(),
            });
        }
    }

    Ok(documents)
}

fn parse_postings_line(line: &str) -> Result<(String, Vec<Posting>)> {
    let (term, encoded_postings) = line
        .split_once('|')
        .with_context(|| format!("invalid postings line for {line}"))?;
    let mut postings = Vec::new();

    if encoded_postings.is_empty() {
        return Ok((term.to_string(), postings));
    }

    for encoded_posting in encoded_postings.split(';') {
        let mut values = encoded_posting.split(',');
        let doc_id = values.next().context("missing document ID")?.parse()?;
        let frequency = values.next().context("missing term frequency")?.parse()?;
        let positions = values.map(str::parse).collect::<Result<Vec<u32>, _>>()?;
        postings.push(Posting {
            doc_id,
            frequency,
            positions,
        });
    }

    Ok((term.to_string(), postings))
}

pub fn read_segment_postings(
    output_dir: &Path,
    segment_id: u32,
) -> Result<HashMap<String, Vec<Posting>>> {
    let binary_path = output_dir.join(format!("seg_{segment_id}.postings.bin"));
    let lexicon_path = output_dir.join(format!("seg_{segment_id}.lexicon.tsv"));
    if binary_path.exists() && lexicon_path.exists() {
        return read_compressed_segment(&binary_path, &lexicon_path);
    }

    let legacy_path = output_dir.join(format!("seg_{segment_id}.postings.txt"));
    let content = std::fs::read_to_string(&legacy_path)
        .with_context(|| format!("failed to read {}", legacy_path.display()))?;
    content
        .lines()
        .map(parse_postings_line)
        .collect::<Result<HashMap<_, _>>>()
}

fn read_compressed_segment(
    postings_path: &Path,
    lexicon_path: &Path,
) -> Result<HashMap<String, Vec<Posting>>> {
    let bytes = std::fs::read(postings_path)
        .with_context(|| format!("failed to read {}", postings_path.display()))?;
    let lexicon = std::fs::read_to_string(lexicon_path)
        .with_context(|| format!("failed to read {}", lexicon_path.display()))?;
    let mut vocabulary = HashMap::new();

    for (line_number, line) in lexicon.lines().enumerate() {
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != 5 {
            anyhow::bail!(
                "invalid lexicon entry at {}:{}",
                lexicon_path.display(),
                line_number + 1
            );
        }

        let offset: usize = fields[1].parse()?;
        let byte_length: usize = fields[2].parse()?;
        let document_frequency: usize = fields[3].parse()?;
        let end = offset
            .checked_add(byte_length)
            .context("postings offset overflow")?;
        let payload = bytes.get(offset..end).with_context(|| {
            format!(
                "postings range {offset}..{end} is outside {}",
                postings_path.display()
            )
        })?;
        vocabulary.insert(
            fields[0].to_string(),
            decode_postings(payload, document_frequency)?,
        );
    }

    Ok(vocabulary)
}

fn read_indexed_paths(output_dir: &Path, segments: &[SegmentMeta]) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for segment in segments {
        paths.extend(
            read_segment_docs(output_dir, segment)?
                .into_iter()
                .map(|document| document.path),
        );
    }
    Ok(paths)
}

pub fn write_manifest(segments: &[SegmentMeta], output_dir: &Path) -> Result<()> {
    write_manifest_to(output_dir.join("manifest.txt"), segments)
}

pub fn write_manifest_atomic(segments: &[SegmentMeta], output_dir: &Path) -> Result<()> {
    let temporary_path = output_dir.join("manifest.tmp");
    write_manifest_to(temporary_path.clone(), segments)?;
    std::fs::rename(temporary_path, output_dir.join("manifest.txt"))?;
    Ok(())
}

fn write_manifest_to(path: PathBuf, segments: &[SegmentMeta]) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    for segment in segments {
        writeln!(
            file,
            "{} {} {}",
            segment.seg_id, segment.start_doc_id, segment.doc_count
        )?;
    }
    Ok(())
}

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
    fn new(output_dir: &Path, existing: &[SegmentMeta]) -> Result<Self> {
        let mut maximum_doc_id = None;
        for segment in existing {
            for document in read_segment_docs(output_dir, segment)? {
                maximum_doc_id = Some(
                    maximum_doc_id
                        .map_or(document.doc_id, |current: u32| current.max(document.doc_id)),
                );
            }
        }

        Ok(Self {
            docs: Vec::new(),
            vocabulary: HashMap::new(),
            next_doc_id: maximum_doc_id.map_or(0, |id| id + 1),
            next_seg_id: existing
                .iter()
                .map(|segment| segment.seg_id)
                .max()
                .map_or(0, |id| id + 1),
            docs_since_last_check: 0,
            output_dir: output_dir.to_path_buf(),
            new_segments: Vec::new(),
        })
    }

    fn merge_document(
        &mut self,
        path: String,
        length: u32,
        postings: PerDocPostings,
    ) -> Result<()> {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;
        self.docs.push(DocMeta {
            doc_id,
            path,
            length,
        });

        for (term, (frequency, positions)) in postings {
            self.vocabulary.entry(term).or_default().push(Posting {
                doc_id,
                frequency,
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
        let start_doc_id = self
            .docs
            .iter()
            .map(|document| document.doc_id)
            .min()
            .unwrap_or(0);
        let doc_count = self.docs.len() as u32;
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

    fn finalize(mut self, existing: Vec<SegmentMeta>) -> Result<(Vec<SegmentMeta>, usize, usize)> {
        self.flush_segment()?;
        let indexed_documents = self
            .new_segments
            .iter()
            .map(|segment| segment.doc_count as usize)
            .sum();
        let segments_written = self.new_segments.len();
        let mut all_segments = existing;
        all_segments.extend(self.new_segments);
        all_segments.sort_by_key(|segment| segment.seg_id);
        Ok((all_segments, indexed_documents, segments_written))
    }
}

pub async fn build_index(root: &Path, output_dir: &Path) -> Result<IndexBuildReport> {
    let started = Instant::now();
    std::fs::create_dir_all(output_dir)?;
    let existing = read_manifest(output_dir)?;
    let previously_indexed_documents = existing
        .iter()
        .map(|segment| segment.doc_count as usize)
        .sum();
    let indexed_paths = read_indexed_paths(output_dir, &existing)?;
    let indexed_paths: std::collections::HashSet<String> = indexed_paths.into_iter().collect();

    let memory_semaphore = Arc::new(Semaphore::new(MAX_MEMORY_MB as usize));
    let task_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));
    let mut tasks: JoinSet<Result<ProcessedDocument>> = JoinSet::new();
    let builder = std::sync::Mutex::new(IndexBuilder::new(output_dir, &existing)?);

    let walker = WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file());

    for entry in walker {
        let path_string = entry.path().to_string_lossy().to_string();
        if indexed_paths.contains(&path_string) {
            continue;
        }

        let path = entry.path().to_path_buf();
        let size = match entry.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                eprintln!("skipping {}: metadata error: {error}", path.display());
                continue;
            }
        };
        let memory_semaphore = memory_semaphore.clone();
        let task_semaphore = task_semaphore.clone();

        tasks.spawn(async move {
            let _task_permit = task_semaphore.acquire().await?;
            if size >= STREAM_THRESHOLD {
                let _memory_permit = memory_semaphore.acquire_many(STREAM_PERMIT_COST).await?;
                process_file_stream(&path)
                    .await
                    .with_context(|| format!("failed to process {}", path.display()))
            } else {
                let permits = ((size / (1024 * 1024)) as u32).clamp(1, MAX_MEMORY_MB);
                let _memory_permit = memory_semaphore.acquire_many(permits).await?;
                process_file_bulk(&path)
                    .await
                    .with_context(|| format!("failed to process {}", path.display()))
            }
        });

        while tasks.len() >= SPAWN_BACKPRESSURE {
            if let Some(result) = tasks.join_next().await {
                ingest_result(result, &builder)?;
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        ingest_result(result, &builder)?;
    }

    let (all_segments, indexed_documents, segments_written) =
        builder.into_inner().unwrap().finalize(existing)?;
    write_manifest(&all_segments, output_dir)?;
    Ok(IndexBuildReport {
        indexed_documents,
        previously_indexed_documents,
        segments_written,
        elapsed_millis: started.elapsed().as_millis(),
    })
}

pub fn merge_segments_background(
    output_dir: &Path,
    segments: &[SegmentMeta],
    new_seg_id: u32,
) -> Result<SegmentMeta> {
    let mut all_documents = Vec::new();
    let mut vocabulary: HashMap<String, Vec<Posting>> = HashMap::new();

    for segment in segments {
        all_documents.extend(read_segment_docs(output_dir, segment)?);
        for (term, mut postings) in read_segment_postings(output_dir, segment.seg_id)? {
            vocabulary.entry(term).or_default().append(&mut postings);
        }
    }

    all_documents.sort_by_key(|document| document.doc_id);
    for postings in vocabulary.values_mut() {
        postings.sort_by_key(|posting| posting.doc_id);
    }
    write_segment_to_disk(new_seg_id, &all_documents, &vocabulary, output_dir)?;

    Ok(SegmentMeta {
        seg_id: new_seg_id,
        start_doc_id: all_documents.first().map_or(0, |document| document.doc_id),
        doc_count: all_documents.len() as u32,
    })
}

fn ingest_result(
    result: std::result::Result<Result<ProcessedDocument>, tokio::task::JoinError>,
    builder: &std::sync::Mutex<IndexBuilder>,
) -> Result<()> {
    match result {
        Ok(Ok((path, length, postings))) => {
            builder
                .lock()
                .unwrap()
                .merge_document(path, length, postings)?;
        }
        Ok(Err(error)) => eprintln!("indexing task error: {error:#}"),
        Err(error) => eprintln!("indexing task panicked: {error}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_index, read_manifest, read_segment_docs};
    use crate::search::SearchIndex;

    #[tokio::test]
    async fn writes_reloadable_documents_with_consistent_lengths() {
        let input = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        std::fs::write(
            input.path().join("one.txt"),
            "Building search engines with Rust.",
        )
        .unwrap();

        let first_build = build_index(input.path(), output.path()).await.unwrap();
        assert_eq!(first_build.indexed_documents, 1);
        assert_eq!(first_build.previously_indexed_documents, 0);
        let second_build = build_index(input.path(), output.path()).await.unwrap();
        assert_eq!(second_build.indexed_documents, 0);
        assert_eq!(second_build.previously_indexed_documents, 1);
        let segments = read_manifest(output.path()).unwrap();
        assert!(output.path().join("seg_0.postings.bin").exists());
        assert!(output.path().join("seg_0.lexicon.tsv").exists());
        let documents = read_segment_docs(output.path(), &segments[0]).unwrap();
        let index = SearchIndex::load(output.path()).unwrap();

        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].length, 5);
        assert_eq!(index.search("build engine", 10).hits.len(), 1);
    }
}
