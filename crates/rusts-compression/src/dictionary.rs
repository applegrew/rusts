//! Dictionary encoding for strings
//!
//! Maps strings to integer IDs for efficient storage.
//! Useful for tag values with low cardinality.

use crate::error::{CompressionError, Result};
use std::collections::HashMap;

/// Dictionary encoder for strings
pub struct DictionaryEncoder {
    /// String to ID mapping
    string_to_id: HashMap<String, u32>,
    /// ID to string mapping (for building the dictionary)
    id_to_string: Vec<String>,
    /// Encoded values (IDs)
    encoded: Vec<u32>,
    /// Maximum dictionary size
    max_size: usize,
}

impl DictionaryEncoder {
    /// Create a new dictionary encoder with default max size (65536)
    pub fn new() -> Self {
        Self::with_max_size(65536)
    }

    /// Create a new dictionary encoder with specified max size
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            string_to_id: HashMap::new(),
            id_to_string: Vec::new(),
            encoded: Vec::new(),
            max_size,
        }
    }

    /// Encode a string value
    pub fn encode(&mut self, value: &str) -> Result<u32> {
        if let Some(&id) = self.string_to_id.get(value) {
            self.encoded.push(id);
            return Ok(id);
        }

        // New string
        if self.id_to_string.len() >= self.max_size {
            return Err(CompressionError::DictionaryFull { max: self.max_size });
        }

        let id = self.id_to_string.len() as u32;
        self.string_to_id.insert(value.to_string(), id);
        self.id_to_string.push(value.to_string());
        self.encoded.push(id);
        Ok(id)
    }

    /// Encode multiple strings
    pub fn encode_all(&mut self, values: &[&str]) -> Result<Vec<u32>> {
        values.iter().map(|v| self.encode(v)).collect()
    }

    /// Get the dictionary (ID to string mapping)
    pub fn dictionary(&self) -> &[String] {
        &self.id_to_string
    }

    /// Get the encoded values
    pub fn encoded_values(&self) -> &[u32] {
        &self.encoded
    }

    /// Get dictionary size
    pub fn dictionary_size(&self) -> usize {
        self.id_to_string.len()
    }

    /// Get number of encoded values
    pub fn count(&self) -> usize {
        self.encoded.len()
    }

    /// Finish encoding and return (dictionary, encoded_ids)
    pub fn finish(self) -> (Vec<String>, Vec<u32>) {
        (self.id_to_string, self.encoded)
    }

    /// Serialize the encoder state to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write dictionary size as u32
        bytes.extend_from_slice(&(self.id_to_string.len() as u32).to_le_bytes());

        // Write each dictionary entry
        for s in &self.id_to_string {
            let s_bytes = s.as_bytes();
            bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(s_bytes);
        }

        // Write encoded values count
        bytes.extend_from_slice(&(self.encoded.len() as u32).to_le_bytes());

        // Write encoded values
        for &id in &self.encoded {
            bytes.extend_from_slice(&id.to_le_bytes());
        }

        bytes
    }
}

impl Default for DictionaryEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Dictionary decoder for strings
pub struct DictionaryDecoder {
    dictionary: Vec<String>,
}

impl DictionaryDecoder {
    /// Create a decoder from a dictionary
    pub fn new(dictionary: Vec<String>) -> Self {
        Self { dictionary }
    }

    /// Create a decoder from serialized bytes
    pub fn from_bytes(data: &[u8]) -> Result<(Self, Vec<u32>)> {
        let mut pos = 0;

        // Read dictionary size
        if data.len() < 4 {
            return Err(CompressionError::BufferUnderflow);
        }
        let dict_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        pos += 4;

        // Read dictionary entries
        let mut dictionary = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            if pos + 4 > data.len() {
                return Err(CompressionError::BufferUnderflow);
            }
            let str_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + str_len > data.len() {
                return Err(CompressionError::BufferUnderflow);
            }
            let s = String::from_utf8(data[pos..pos + str_len].to_vec())
                .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
            dictionary.push(s);
            pos += str_len;
        }

        // Read encoded values count
        if pos + 4 > data.len() {
            return Err(CompressionError::BufferUnderflow);
        }
        let values_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Read encoded values
        let mut encoded = Vec::with_capacity(values_count);
        for _ in 0..values_count {
            if pos + 4 > data.len() {
                return Err(CompressionError::BufferUnderflow);
            }
            let id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            encoded.push(id);
            pos += 4;
        }

        Ok((Self { dictionary }, encoded))
    }

    /// Decode an ID to string
    pub fn decode(&self, id: u32) -> Result<&str> {
        self.dictionary.get(id as usize).map(|s| s.as_str()).ok_or(
            CompressionError::InvalidDictionaryIndex {
                index: id,
                max: self.dictionary.len() as u32,
            },
        )
    }

    /// Decode all IDs
    pub fn decode_all(&self, ids: &[u32]) -> Result<Vec<&str>> {
        ids.iter().map(|&id| self.decode(id)).collect()
    }

    /// Get dictionary size
    pub fn dictionary_size(&self) -> usize {
        self.dictionary.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_encoding() {
        let mut encoder = DictionaryEncoder::new();

        let id1 = encoder.encode("hello").unwrap();
        let id2 = encoder.encode("world").unwrap();
        let id3 = encoder.encode("hello").unwrap(); // duplicate

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 0); // same as id1

        assert_eq!(encoder.dictionary_size(), 2);
        assert_eq!(encoder.count(), 3);
    }

    #[test]
    fn test_encoding_decoding_roundtrip() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["foo", "bar", "baz", "foo", "bar", "foo"];

        encoder.encode_all(&values).unwrap();
        let (dictionary, encoded) = encoder.finish();

        let decoder = DictionaryDecoder::new(dictionary);
        let decoded = decoder.decode_all(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["server01", "server02", "server01", "server03", "server02"];

        encoder.encode_all(&values).unwrap();
        let bytes = encoder.to_bytes();

        let (decoder, encoded) = DictionaryDecoder::from_bytes(&bytes).unwrap();
        let decoded = decoder.decode_all(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_empty_strings() {
        let mut encoder = DictionaryEncoder::new();
        encoder.encode("").unwrap();
        encoder.encode("non-empty").unwrap();
        encoder.encode("").unwrap();

        let (dictionary, encoded) = encoder.finish();
        let decoder = DictionaryDecoder::new(dictionary);

        assert_eq!(decoder.decode(encoded[0]).unwrap(), "");
        assert_eq!(decoder.decode(encoded[1]).unwrap(), "non-empty");
        assert_eq!(decoder.decode(encoded[2]).unwrap(), "");
    }

    #[test]
    fn test_unicode() {
        let mut encoder = DictionaryEncoder::new();
        let values = vec!["héllo", "世界", "🚀", "héllo"];

        encoder.encode_all(&values).unwrap();
        let bytes = encoder.to_bytes();

        let (decoder, encoded) = DictionaryDecoder::from_bytes(&bytes).unwrap();
        let decoded = decoder.decode_all(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_dictionary_full() {
        let mut encoder = DictionaryEncoder::with_max_size(3);

        encoder.encode("a").unwrap();
        encoder.encode("b").unwrap();
        encoder.encode("c").unwrap();

        // This should work (duplicate)
        encoder.encode("a").unwrap();

        // This should fail (new entry when full)
        let result = encoder.encode("d");
        assert!(matches!(result, Err(CompressionError::DictionaryFull { max: 3 })));
    }

    #[test]
    fn test_high_cardinality_compression() {
        let mut encoder = DictionaryEncoder::new();

        // Simulate tag values like hostnames
        let hosts: Vec<String> = (0..100).map(|i| format!("server{:03}", i)).collect();

        // Each hostname appears multiple times
        for _ in 0..100 {
            for host in &hosts {
                encoder.encode(host).unwrap();
            }
        }

        let bytes = encoder.to_bytes();

        // 10000 values, each ~10 chars = ~100KB uncompressed
        // Dictionary: 100 entries * ~10 chars = ~1KB
        // Values: 10000 * 4 bytes = 40KB
        // Total: ~41KB vs ~100KB = ~2.5x compression
        let uncompressed_size: usize = hosts.iter().map(|s| s.len()).sum::<usize>() * 100;
        let compression_ratio = uncompressed_size as f64 / bytes.len() as f64;

        assert!(
            compression_ratio > 2.0,
            "Expected >2x compression, got {}x",
            compression_ratio
        );
    }

    #[test]
    fn test_invalid_decode() {
        let decoder = DictionaryDecoder::new(vec!["a".to_string(), "b".to_string()]);

        assert!(decoder.decode(0).is_ok());
        assert!(decoder.decode(1).is_ok());
        assert!(decoder.decode(2).is_err());
    }
}
