use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};
use std::sync::LazyLock;

static NON_WORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[^a-zA-Z0-9\s]+").expect("token regex must compile"));

/// Normalizes text into lowercase, English-stemmed terms used by both indexing and queries.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    let lowercase = text.to_lowercase();
    let cleaned = NON_WORD.replace_all(&lowercase, " ");

    cleaned
        .split_whitespace()
        .map(|word| stemmer.stem(word).into_owned())
        .filter(|word| !word.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::analyze;

    #[test]
    fn normalizes_case_punctuation_and_word_forms() {
        assert_eq!(
            analyze("Building SEARCH-engines, searched!"),
            vec!["build", "search", "engin", "search"]
        );
    }
}
