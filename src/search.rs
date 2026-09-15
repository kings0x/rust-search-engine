use crate::analysis::analyze;
use crate::storage::ingest::{
    DocMeta, Posting, read_manifest, read_segment_docs, read_segment_postings,
};
use anyhow::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

const DEFAULT_K1: f64 = 1.2;
const DEFAULT_B: f64 = 0.75;

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub doc_id: u32,
    pub path: String,
    pub score: f64,
    pub document_length: u32,
    pub matched_terms: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub analyzed_terms: Vec<String>,
    pub total_candidates: usize,
    pub took_micros: u128,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexStats {
    pub documents: usize,
    pub terms: usize,
    pub postings: usize,
    pub average_document_length: f64,
}

#[derive(Debug, Clone)]
pub struct SearchIndex {
    documents: HashMap<u32, DocMeta>,
    postings: HashMap<String, Vec<Posting>>,
    average_document_length: f64,
    k1: f64,
    b: f64,
}

impl SearchIndex {
    /// Loads every persisted segment into a read-optimized in-memory index.
    pub fn load(output_dir: &Path) -> Result<Self> {
        let segments = read_manifest(output_dir)?;
        let mut documents = HashMap::new();
        let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();

        for segment in &segments {
            for document in read_segment_docs(output_dir, segment)? {
                documents.insert(document.doc_id, document);
            }

            for (term, mut term_postings) in read_segment_postings(output_dir, segment.seg_id)? {
                postings.entry(term).or_default().append(&mut term_postings);
            }
        }

        for term_postings in postings.values_mut() {
            term_postings.sort_by_key(|posting| posting.doc_id);
        }

        let average_document_length = if documents.is_empty() {
            0.0
        } else {
            documents
                .values()
                .map(|document| document.length as u64)
                .sum::<u64>() as f64
                / documents.len() as f64
        };

        Ok(Self {
            documents,
            postings,
            average_document_length,
            k1: DEFAULT_K1,
            b: DEFAULT_B,
        })
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            documents: self.documents.len(),
            terms: self.postings.len(),
            postings: self.postings.values().map(Vec::len).sum(),
            average_document_length: self.average_document_length,
        }
    }

    /// Executes an OR query and ranks matching documents with BM25.
    pub fn search(&self, query: &str, limit: usize) -> SearchResponse {
        let started = Instant::now();
        let mut analyzed_terms = analyze(query);
        let mut seen = HashSet::new();
        analyzed_terms.retain(|term| seen.insert(term.clone()));

        let document_count = self.documents.len() as f64;
        let mut scores: HashMap<u32, (f64, usize)> = HashMap::new();

        if document_count > 0.0 && self.average_document_length > 0.0 {
            for term in &analyzed_terms {
                let Some(term_postings) = self.postings.get(term) else {
                    continue;
                };

                let document_frequency = term_postings.len() as f64;
                let inverse_document_frequency = (1.0
                    + (document_count - document_frequency + 0.5) / (document_frequency + 0.5))
                    .ln();

                for posting in term_postings {
                    let Some(document) = self.documents.get(&posting.doc_id) else {
                        continue;
                    };

                    let term_frequency = posting.frequency as f64;
                    let length_ratio = document.length as f64 / self.average_document_length;
                    let denominator =
                        term_frequency + self.k1 * (1.0 - self.b + self.b * length_ratio);
                    let term_score = inverse_document_frequency
                        * (term_frequency * (self.k1 + 1.0) / denominator);

                    let entry = scores.entry(posting.doc_id).or_default();
                    entry.0 += term_score;
                    entry.1 += 1;
                }
            }
        }

        let total_candidates = scores.len();
        let mut hits: Vec<SearchHit> = scores
            .into_iter()
            .filter_map(|(doc_id, (score, matched_terms))| {
                self.documents.get(&doc_id).map(|document| SearchHit {
                    doc_id,
                    path: document.path.clone(),
                    score,
                    document_length: document.length,
                    matched_terms,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.doc_id.cmp(&right.doc_id))
        });
        hits.truncate(limit.clamp(1, 100));

        SearchResponse {
            query: query.to_string(),
            analyzed_terms,
            total_candidates,
            took_micros: started.elapsed().as_micros(),
            hits,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SearchIndex;
    use crate::storage::ingest::{DocMeta, Posting};
    use std::collections::HashMap;

    fn posting(doc_id: u32, frequency: u32) -> Posting {
        Posting {
            doc_id,
            frequency,
            positions: Vec::new(),
        }
    }

    #[test]
    fn ranks_a_document_matching_more_query_terms_first() {
        let documents = HashMap::from([
            (
                0,
                DocMeta {
                    doc_id: 0,
                    path: "rust-search.md".into(),
                    length: 100,
                },
            ),
            (
                1,
                DocMeta {
                    doc_id: 1,
                    path: "rust-only.md".into(),
                    length: 100,
                },
            ),
            (
                2,
                DocMeta {
                    doc_id: 2,
                    path: "unrelated.md".into(),
                    length: 100,
                },
            ),
        ]);
        let postings = HashMap::from([
            ("rust".into(), vec![posting(0, 2), posting(1, 4)]),
            ("search".into(), vec![posting(0, 3)]),
        ]);
        let index = SearchIndex {
            documents,
            postings,
            average_document_length: 100.0,
            k1: 1.2,
            b: 0.75,
        };

        let response = index.search("Rust search", 10);

        assert_eq!(response.hits.len(), 2);
        assert_eq!(response.hits[0].path, "rust-search.md");
        assert_eq!(response.hits[0].matched_terms, 2);
        assert!(response.hits[0].score > response.hits[1].score);
    }

    #[test]
    fn empty_and_unknown_queries_return_no_hits() {
        let index = SearchIndex {
            documents: HashMap::new(),
            postings: HashMap::new(),
            average_document_length: 0.0,
            k1: 1.2,
            b: 0.75,
        };

        assert!(index.search("", 10).hits.is_empty());
        assert!(index.search("missing", 10).hits.is_empty());
    }
}
