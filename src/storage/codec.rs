use super::ingest::Posting;
use anyhow::{Context, Result};

/// Encodes sorted postings with document/position gaps and variable-byte integers.
pub fn encode_postings(postings: &[Posting]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut previous_doc_id = 0_u32;

    for posting in postings {
        encode_u32(posting.doc_id - previous_doc_id, &mut output);
        encode_u32(posting.frequency, &mut output);
        encode_u32(posting.positions.len() as u32, &mut output);

        let mut previous_position = 0_u32;
        for position in &posting.positions {
            encode_u32(*position - previous_position, &mut output);
            previous_position = *position;
        }
        previous_doc_id = posting.doc_id;
    }

    output
}

/// Decodes a known number of postings from a delta + variable-byte payload.
pub fn decode_postings(bytes: &[u8], posting_count: usize) -> Result<Vec<Posting>> {
    let mut cursor = 0_usize;
    let mut previous_doc_id = 0_u32;
    let mut postings = Vec::with_capacity(posting_count);

    for _ in 0..posting_count {
        let doc_id = previous_doc_id + decode_u32(bytes, &mut cursor)?;
        let frequency = decode_u32(bytes, &mut cursor)?;
        let position_count = decode_u32(bytes, &mut cursor)? as usize;
        let mut positions = Vec::with_capacity(position_count);
        let mut previous_position = 0_u32;

        for _ in 0..position_count {
            let position = previous_position + decode_u32(bytes, &mut cursor)?;
            positions.push(position);
            previous_position = position;
        }

        postings.push(Posting {
            doc_id,
            frequency,
            positions,
        });
        previous_doc_id = doc_id;
    }

    if cursor != bytes.len() {
        anyhow::bail!(
            "postings payload contains {} trailing bytes",
            bytes.len() - cursor
        );
    }

    Ok(postings)
}

fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
    let mut groups = [0_u8; 5];
    let mut count = 0;
    loop {
        groups[count] = (value & 0x7f) as u8;
        count += 1;
        value >>= 7;
        if value == 0 {
            break;
        }
    }

    for index in (0..count).rev() {
        let terminal = index == 0;
        output.push(groups[index] | if terminal { 0x80 } else { 0 });
    }
}

fn decode_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let mut value = 0_u32;
    for _ in 0..5 {
        let byte = *bytes
            .get(*cursor)
            .context("truncated variable-byte integer")?;
        *cursor += 1;
        value = value
            .checked_mul(128)
            .and_then(|current| current.checked_add((byte & 0x7f) as u32))
            .context("variable-byte integer overflow")?;
        if byte & 0x80 != 0 {
            return Ok(value);
        }
    }
    anyhow::bail!("variable-byte integer exceeds u32 encoding length")
}

#[cfg(test)]
mod tests {
    use super::{decode_postings, encode_postings};
    use crate::storage::ingest::Posting;

    #[test]
    fn round_trips_delta_encoded_postings() {
        let postings = vec![
            Posting {
                doc_id: 100,
                frequency: 3,
                positions: vec![2, 8, 144],
            },
            Posting {
                doc_id: 105,
                frequency: 2,
                positions: vec![1, 9],
            },
            Posting {
                doc_id: 10_000,
                frequency: 1,
                positions: vec![65_000],
            },
        ];

        let encoded = encode_postings(&postings);
        let decoded = decode_postings(&encoded, postings.len()).unwrap();

        assert_eq!(decoded, postings);
        assert!(encoded.len() < 12 * postings.len() + 4 * 6);
    }

    #[test]
    fn rejects_truncated_payloads() {
        let error = decode_postings(&[0x01], 1).unwrap_err();
        assert!(error.to_string().contains("truncated"));
    }
}
