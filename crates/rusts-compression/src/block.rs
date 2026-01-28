//! Block compression using LZ4 and Zstd
//!
//! Provides additional compression on top of specialized encoders.
//! - LZ4: Fast compression, good for hot data
//! - Zstd: Better compression ratio, good for cold data

use crate::error::{CompressionError, Result};

/// Compression level presets
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionLevel {
    /// No compression
    None,
    /// LZ4 fast compression (default for hot tier)
    Fast,
    /// LZ4 high compression
    Default,
    /// Zstd compression level 1
    Balanced,
    /// Zstd compression level 3 (default)
    High,
    /// Zstd compression level 9 (best ratio)
    Best,
}

impl CompressionLevel {
    /// Get the algorithm for this level
    pub fn algorithm(&self) -> CompressionAlgorithm {
        match self {
            CompressionLevel::None => CompressionAlgorithm::None,
            CompressionLevel::Fast | CompressionLevel::Default => CompressionAlgorithm::Lz4,
            CompressionLevel::Balanced | CompressionLevel::High | CompressionLevel::Best => {
                CompressionAlgorithm::Zstd
            }
        }
    }
}

/// Compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    /// Get the magic byte for this algorithm
    pub fn magic_byte(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0x00,
            CompressionAlgorithm::Lz4 => 0x01,
            CompressionAlgorithm::Zstd => 0x02,
        }
    }

    /// Parse magic byte
    pub fn from_magic_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(CompressionAlgorithm::None),
            0x01 => Ok(CompressionAlgorithm::Lz4),
            0x02 => Ok(CompressionAlgorithm::Zstd),
            _ => Err(CompressionError::InvalidData(format!(
                "Unknown compression algorithm: {}",
                byte
            ))),
        }
    }
}

/// Block compressor
pub struct BlockCompressor {
    level: CompressionLevel,
}

impl BlockCompressor {
    /// Create a new compressor with the given level
    pub fn new(level: CompressionLevel) -> Self {
        Self { level }
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.level {
            CompressionLevel::None => {
                let mut result = Vec::with_capacity(data.len() + 1);
                result.push(CompressionAlgorithm::None.magic_byte());
                result.extend_from_slice(data);
                Ok(result)
            }
            CompressionLevel::Fast => self.compress_lz4_fast(data),
            CompressionLevel::Default => self.compress_lz4_hc(data),
            CompressionLevel::Balanced => self.compress_zstd(data, 1),
            CompressionLevel::High => self.compress_zstd(data, 3),
            CompressionLevel::Best => self.compress_zstd(data, 9),
        }
    }

    fn compress_lz4_fast(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed = lz4::block::compress(data, None, false)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

        let mut result = Vec::with_capacity(compressed.len() + 5);
        result.push(CompressionAlgorithm::Lz4.magic_byte());
        // Store original size for decompression
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());
        result.extend_from_slice(&compressed);
        Ok(result)
    }

    fn compress_lz4_hc(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed = lz4::block::compress(data, Some(lz4::block::CompressionMode::HIGHCOMPRESSION(9)), false)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

        let mut result = Vec::with_capacity(compressed.len() + 5);
        result.push(CompressionAlgorithm::Lz4.magic_byte());
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());
        result.extend_from_slice(&compressed);
        Ok(result)
    }

    fn compress_zstd(&self, data: &[u8], level: i32) -> Result<Vec<u8>> {
        let compressed = zstd::encode_all(data, level)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

        let mut result = Vec::with_capacity(compressed.len() + 1);
        result.push(CompressionAlgorithm::Zstd.magic_byte());
        result.extend_from_slice(&compressed);
        Ok(result)
    }
}

impl Default for BlockCompressor {
    fn default() -> Self {
        Self::new(CompressionLevel::Default)
    }
}

/// Decompress data
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(CompressionError::BufferUnderflow);
    }

    let algorithm = CompressionAlgorithm::from_magic_byte(data[0])?;

    match algorithm {
        CompressionAlgorithm::None => Ok(data[1..].to_vec()),
        CompressionAlgorithm::Lz4 => {
            if data.len() < 5 {
                return Err(CompressionError::BufferUnderflow);
            }
            let original_size = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            lz4::block::decompress(&data[5..], Some(original_size as i32))
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
        }
        CompressionAlgorithm::Zstd => zstd::decode_all(&data[1..])
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let compressor = BlockCompressor::new(CompressionLevel::None);
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed[0], 0x00);
        assert_eq!(&compressed[1..], data);

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compression() {
        let compressor = BlockCompressor::new(CompressionLevel::Fast);
        let data = "Hello, World! ".repeat(100);

        let compressed = compressor.compress(data.as_bytes()).unwrap();
        assert_eq!(compressed[0], 0x01);
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data.as_bytes());
    }

    #[test]
    fn test_lz4_hc_compression() {
        let compressor = BlockCompressor::new(CompressionLevel::Default);
        let data = "Hello, World! ".repeat(100);

        let compressed = compressor.compress(data.as_bytes()).unwrap();
        assert_eq!(compressed[0], 0x01);
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data.as_bytes());
    }

    #[test]
    fn test_zstd_compression() {
        for level in [
            CompressionLevel::Balanced,
            CompressionLevel::High,
            CompressionLevel::Best,
        ] {
            let compressor = BlockCompressor::new(level);
            let data = "Hello, World! ".repeat(100);

            let compressed = compressor.compress(data.as_bytes()).unwrap();
            assert_eq!(compressed[0], 0x02);
            assert!(compressed.len() < data.len());

            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(decompressed, data.as_bytes());
        }
    }

    #[test]
    fn test_empty_data() {
        let compressor = BlockCompressor::new(CompressionLevel::Fast);
        let data = b"";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_ratio_comparison() {
        // Generate some compressible data
        let data: Vec<u8> = (0..10000)
            .map(|i| ((i % 256) as u8).wrapping_add((i / 1000) as u8))
            .collect();

        let levels = [
            CompressionLevel::None,
            CompressionLevel::Fast,
            CompressionLevel::Default,
            CompressionLevel::Balanced,
            CompressionLevel::High,
            CompressionLevel::Best,
        ];

        let mut prev_size = usize::MAX;
        for level in levels {
            let compressor = BlockCompressor::new(level);
            let compressed = compressor.compress(&data).unwrap();

            // Verify roundtrip
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(decompressed, data);

            // Generally, higher compression levels shouldn't produce larger output
            // (though this isn't strictly guaranteed for all data)
            if level != CompressionLevel::None {
                assert!(
                    compressed.len() <= prev_size || prev_size == data.len() + 1,
                    "Level {:?} produced larger output ({}) than previous ({})",
                    level,
                    compressed.len(),
                    prev_size
                );
            }
            prev_size = compressed.len();
        }
    }

    #[test]
    fn test_random_data() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let data: Vec<u8> = (0..10000).map(|_| rng.gen()).collect();

        for level in [CompressionLevel::Fast, CompressionLevel::High] {
            let compressor = BlockCompressor::new(level);
            let compressed = compressor.compress(&data).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            assert_eq!(decompressed, data);
        }
    }
}
