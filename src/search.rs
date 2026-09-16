use crate::analysis::analyze;
use crate::storage::ingest::{
    DocMeta, Posting, read_manifest, read_segment_docs, read_segment_postings,
};
use anyhow::Result;
use serde::Serialize;
use std::cell::RefCell;
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
    pub suggestions: Vec<TermSuggestion>,
    pub total_candidates: usize,
    pub took_micros: u128,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TermSuggestion {
    pub term: String,
    pub replacement: String,
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
    documents: Vec<Option<DocMeta>>,
    document_count: usize,
    postings: HashMap<String, Vec<RankedPosting>>,
    trigram_terms: HashMap<String, Vec<String>>,
    average_document_length: f64,
}

#[derive(Debug, Clone)]
struct RankedPosting {
    doc_id: u32,
    bm25_weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct ScoredCandidate {
    doc_id: u32,
    score: f64,
    matched_terms: usize,
}

#[derive(Default)]
struct QueryScratch {
    scores: Vec<f64>,
    matched_terms: Vec<usize>,
    generations: Vec<u32>,
    generation: u32,
    touched: Vec<u32>,
    candidates: Vec<ScoredCandidate>,
}

thread_local! {
    static QUERY_SCRATCH: RefCell<QueryScratch> = RefCell::new(QueryScratch::default());
}

impl SearchIndex {
    /// Loads every persisted segment into a read-optimized in-memory index.
    pub fn load(output_dir: &Path) -> Result<Self> {
        let segments = read_manifest(output_dir)?;
        let mut loaded_documents = Vec::new();
        let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();

        for segment in &segments {
            for document in read_segment_docs(output_dir, segment)? {
                loaded_documents.push(document);
            }

            for (term, mut term_postings) in read_segment_postings(output_dir, segment.seg_id)? {
                postings.entry(term).or_default().append(&mut term_postings);
            }
        }

        for term_postings in postings.values_mut() {
            term_postings.sort_by_key(|posting| posting.doc_id);
        }

        let document_count = loaded_documents.len();
        let average_document_length = if loaded_documents.is_empty() {
            0.0
        } else {
            loaded_documents
                .iter()
                .map(|document| document.length as u64)
                .sum::<u64>() as f64
                / document_count as f64
        };
        let maximum_doc_id = loaded_documents
            .iter()
            .map(|document| document.doc_id as usize)
            .max()
            .unwrap_or(0);
        let mut documents = vec![None; maximum_doc_id + usize::from(document_count > 0)];
        for document in loaded_documents {
            let doc_id = document.doc_id as usize;
            documents[doc_id] = Some(document);
        }
        let document_norms: Vec<f64> = documents
            .iter()
            .map(|document| {
                document.as_ref().map_or(0.0, |document| {
                    let length_ratio = document.length as f64 / average_document_length;
                    DEFAULT_K1 * (1.0 - DEFAULT_B + DEFAULT_B * length_ratio)
                })
            })
            .collect();
        let postings = rank_postings(postings, document_count, &document_norms);
        let trigram_terms = build_trigram_index(postings.keys());

        Ok(Self {
            documents,
            document_count,
            postings,
            trigram_terms,
            average_document_length,
        })
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            documents: self.document_count,
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
        let suggestions = analyzed_terms
            .iter()
            .filter(|term| !self.postings.contains_key(*term))
            .filter_map(|term| {
                self.suggest_term(term).map(|replacement| TermSuggestion {
                    term: term.clone(),
                    replacement,
                })
            })
            .collect();

        let result_limit = limit.clamp(1, 100);
        let document_count = self.document_count as f64;
        let (total_candidates, hits) = QUERY_SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            scratch.begin(self.documents.len());

            if document_count > 0.0 && self.average_document_length > 0.0 {
                for term in &analyzed_terms {
                    let Some(term_postings) = self.postings.get(term) else {
                        continue;
                    };

                    for posting in term_postings {
                        let doc_id = posting.doc_id as usize;
                        let Some(_) = self.documents.get(doc_id).and_then(Option::as_ref) else {
                            continue;
                        };

                        if scratch.generations[doc_id] != scratch.generation {
                            scratch.generations[doc_id] = scratch.generation;
                            scratch.scores[doc_id] = 0.0;
                            scratch.matched_terms[doc_id] = 0;
                            scratch.touched.push(posting.doc_id);
                        }

                        scratch.scores[doc_id] += posting.bm25_weight;
                        scratch.matched_terms[doc_id] += 1;
                    }
                }
            }

            for touched_index in 0..scratch.touched.len() {
                let doc_id = scratch.touched[touched_index];
                let index = doc_id as usize;
                let score = scratch.scores[index];
                let matched_terms = scratch.matched_terms[index];
                scratch.candidates.push(ScoredCandidate {
                    doc_id,
                    score,
                    matched_terms,
                });
            }
            let total_candidates = scratch.candidates.len();
            if total_candidates > result_limit {
                scratch
                    .candidates
                    .select_nth_unstable_by(result_limit, compare_candidates);
                scratch.candidates.truncate(result_limit);
            }
            scratch.candidates.sort_unstable_by(compare_candidates);

            let hits = scratch
                .candidates
                .iter()
                .filter_map(|candidate| {
                    self.documents[candidate.doc_id as usize]
                        .as_ref()
                        .map(|document| SearchHit {
                            doc_id: candidate.doc_id,
                            path: document.path.clone(),
                            score: candidate.score,
                            document_length: document.length,
                            matched_terms: candidate.matched_terms,
                        })
                })
                .collect();
            (total_candidates, hits)
        });

        SearchResponse {
            query: query.to_string(),
            analyzed_terms,
            suggestions,
            total_candidates,
            took_micros: started.elapsed().as_micros(),
            hits,
        }
    }

    fn suggest_term(&self, term: &str) -> Option<String> {
        let query_trigrams = trigrams(term);
        let mut overlaps: HashMap<&str, usize> = HashMap::new();
        for trigram in &query_trigrams {
            if let Some(candidates) = self.trigram_terms.get(trigram) {
                for candidate in candidates {
                    *overlaps.entry(candidate).or_default() += 1;
                }
            }
        }

        overlaps
            .into_iter()
            .filter_map(|(candidate, overlap)| {
                let candidate_trigram_count = trigrams(candidate).len();
                let similarity =
                    2.0 * overlap as f64 / (query_trigrams.len() + candidate_trigram_count) as f64;
                let distance = edit_distance(term, candidate);
                let maximum_distance = 2_usize.max(term.chars().count() / 3);
                (similarity >= 0.35 && distance <= maximum_distance)
                    .then_some((candidate, similarity, distance))
            })
            .max_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| right.2.cmp(&left.2))
                    .then_with(|| right.0.cmp(left.0))
            })
            .map(|(candidate, _, _)| candidate.to_string())
    }
}

impl QueryScratch {
    fn begin(&mut self, document_slots: usize) {
        if self.scores.len() < document_slots {
            self.scores.resize(document_slots, 0.0);
            self.matched_terms.resize(document_slots, 0);
            self.generations.resize(document_slots, 0);
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.generations.fill(0);
            self.generation = 1;
        }
        self.touched.clear();
        self.candidates.clear();
    }
}

fn compare_candidates(left: &ScoredCandidate, right: &ScoredCandidate) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.doc_id.cmp(&right.doc_id))
}

fn build_trigram_index<'a>(
    terms: impl Iterator<Item = &'a String>,
) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for term in terms {
        for trigram in trigrams(term) {
            index.entry(trigram).or_default().push(term.clone());
        }
    }
    index
}

fn rank_postings(
    postings: HashMap<String, Vec<Posting>>,
    document_count: usize,
    document_norms: &[f64],
) -> HashMap<String, Vec<RankedPosting>> {
    let document_count = document_count as f64;
    postings
        .into_iter()
        .map(|(term, term_postings)| {
            let document_frequency = term_postings.len() as f64;
            let inverse_document_frequency = (1.0
                + (document_count - document_frequency + 0.5) / (document_frequency + 0.5))
                .ln();
            let ranked = term_postings
                .into_iter()
                .filter_map(|posting| {
                    let normalization = *document_norms.get(posting.doc_id as usize)?;
                    let term_frequency = posting.frequency as f64;
                    Some(RankedPosting {
                        doc_id: posting.doc_id,
                        bm25_weight: inverse_document_frequency
                            * (term_frequency * (DEFAULT_K1 + 1.0)
                                / (term_frequency + normalization)),
                    })
                })
                .collect();
            (term, ranked)
        })
        .collect()
}

fn trigrams(term: &str) -> HashSet<String> {
    let padded: Vec<char> = format!("^{term}$").chars().collect();
    if padded.len() < 3 {
        return HashSet::from([padded.iter().collect()]);
    }
    padded
        .windows(3)
        .map(|window| window.iter().collect())
        .collect()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();

    for (left_index, left_character) in left.chars().enumerate() {
        let mut current = Vec::with_capacity(right.len() + 1);
        current.push(left_index + 1);
        for (right_index, right_character) in right.iter().enumerate() {
            let substitution =
                previous[right_index] + usize::from(left_character != *right_character);
            current.push(
                (previous[right_index + 1] + 1)
                    .min(current[right_index] + 1)
                    .min(substitution),
            );
        }
        previous = current;
    }

    previous[right.len()]
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
        let documents = vec![
            Some(DocMeta {
                doc_id: 0,
                path: "rust-search.md".into(),
                length: 100,
            }),
            Some(DocMeta {
                doc_id: 1,
                path: "rust-only.md".into(),
                length: 100,
            }),
            Some(DocMeta {
                doc_id: 2,
                path: "unrelated.md".into(),
                length: 100,
            }),
        ];
        let postings = HashMap::from([
            ("rust".into(), vec![posting(0, 2), posting(1, 4)]),
            ("search".into(), vec![posting(0, 3)]),
        ]);
        let trigram_terms = super::build_trigram_index(postings.keys());
        let index = SearchIndex {
            documents,
            document_count: 3,
            postings: super::rank_postings(postings, 3, &[1.2, 1.2, 1.2]),
            trigram_terms,
            average_document_length: 100.0,
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
            documents: Vec::new(),
            document_count: 0,
            postings: HashMap::new(),
            trigram_terms: HashMap::new(),
            average_document_length: 0.0,
        };

        assert!(index.search("", 10).hits.is_empty());
        assert!(index.search("missing", 10).hits.is_empty());
    }

    #[test]
    fn suggests_a_close_vocabulary_term_for_a_typo() {
        let postings = HashMap::from([("search".into(), vec![posting(0, 1)])]);
        let trigram_terms = super::build_trigram_index(postings.keys());
        let index = SearchIndex {
            documents: vec![Some(DocMeta {
                doc_id: 0,
                path: "search.md".into(),
                length: 1,
            })],
            document_count: 1,
            postings: super::rank_postings(postings, 1, &[1.2]),
            trigram_terms,
            average_document_length: 1.0,
        };

        let response = index.search("serch", 10);

        assert_eq!(response.suggestions.len(), 1);
        assert_eq!(response.suggestions[0].term, "serch");
        assert_eq!(response.suggestions[0].replacement, "search");
    }
}
