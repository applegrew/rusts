//! Gorilla XOR compression for floating-point values
//!
//! Implements Facebook's Gorilla algorithm for compressing floating-point time series.
//! The algorithm exploits the fact that consecutive float values in time series
//! often have similar binary representations.
//!
//! Reference: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use crate::error::{CompressionError, Result};

/// Gorilla XOR encoder for floating-point values
pub struct GorillaEncoder {
    /// Output buffer
    buffer: Vec<u8>,
    /// Current bit position in the last byte
    bit_pos: u8,
    /// Previous value (as bits)
    prev_value: u64,
    /// Previous leading zeros count
    prev_leading: u8,
    /// Previous trailing zeros count
    prev_trailing: u8,
    /// Number of values encoded
    count: usize,
}

impl GorillaEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
            bit_pos: 0,
            prev_value: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            count: 0,
        }
    }

    /// Create encoder with capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            bit_pos: 0,
            prev_value: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            count: 0,
        }
    }

    /// Encode a floating-point value
    pub fn encode(&mut self, value: f64) {
        let bits = value.to_bits();

        if self.count == 0 {
            // First value: store uncompressed
            self.write_bits(bits, 64);
            self.prev_value = bits;
            self.count = 1;
            return;
        }

        let xor = self.prev_value ^ bits;

        if xor == 0 {
            // Same value: write single 0 bit
            self.write_bit(false);
        } else {
            // Different value: write 1 bit
            self.write_bit(true);

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            // Cap leading zeros to 31 (5 bits)
            let leading = leading.min(31);

            if leading >= self.prev_leading && trailing >= self.prev_trailing {
                // Use previous block info: write 0 bit + meaningful bits
                self.write_bit(false);
                let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
                let meaningful = (xor >> self.prev_trailing) as u64;
                self.write_bits(meaningful, meaningful_bits as usize);
            } else {
                // New block info: write 1 bit + 5 bits leading + 6 bits length + meaningful bits
                self.write_bit(true);
                self.write_bits(leading as u64, 5);

                let meaningful_bits = 64 - leading - trailing;
                // Length is meaningful_bits - 1 (since 0 meaningful bits is impossible)
                self.write_bits((meaningful_bits - 1) as u64, 6);

                let meaningful = (xor >> trailing) as u64;
                self.write_bits(meaningful, meaningful_bits as usize);

                self.prev_leading = leading;
                self.prev_trailing = trailing;
            }
        }

        self.prev_value = bits;
        self.count += 1;
    }

    /// Finish encoding and return the compressed bytes
    pub fn finish(mut self) -> Vec<u8> {
        // Pad the last byte if needed
        if self.bit_pos > 0 {
            // Byte is already in buffer, just ensure we're aligned
        }
        self.buffer
    }

    /// Get the number of values encoded
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get current compressed size in bytes
    pub fn compressed_size(&self) -> usize {
        self.buffer.len()
    }

    fn write_bit(&mut self, bit: bool) {
        if self.bit_pos == 0 {
            self.buffer.push(0);
        }

        if bit {
            let idx = self.buffer.len() - 1;
            self.buffer[idx] |= 1 << (7 - self.bit_pos);
        }

        self.bit_pos = (self.bit_pos + 1) % 8;
    }

    fn write_bits(&mut self, value: u64, num_bits: usize) {
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Gorilla XOR decoder for floating-point values
pub struct GorillaDecoder<'a> {
    /// Input data
    data: &'a [u8],
    /// Current byte index
    byte_idx: usize,
    /// Current bit position in the byte
    bit_pos: u8,
    /// Previous value (as bits)
    prev_value: u64,
    /// Previous leading zeros count
    prev_leading: u8,
    /// Previous trailing zeros count
    prev_trailing: u8,
    /// Number of values decoded
    count: usize,
    /// Expected total values
    expected_count: usize,
}

impl<'a> GorillaDecoder<'a> {
    /// Create a new decoder
    pub fn new(data: &'a [u8], expected_count: usize) -> Self {
        Self {
            data,
            byte_idx: 0,
            bit_pos: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            count: 0,
            expected_count,
        }
    }

    /// Decode the next value
    pub fn decode(&mut self) -> Result<Option<f64>> {
        if self.count >= self.expected_count {
            return Ok(None);
        }

        let bits = if self.count == 0 {
            // First value: read 64 bits
            self.read_bits(64)?
        } else {
            let same = !self.read_bit()?;

            if same {
                // Same value
                self.prev_value
            } else {
                let new_block = self.read_bit()?;

                let (leading, meaningful_bits) = if new_block {
                    // New block info
                    let leading = self.read_bits(5)? as u8;
                    let meaningful_bits = self.read_bits(6)? as u8 + 1;
                    self.prev_leading = leading;
                    self.prev_trailing = 64 - leading - meaningful_bits;
                    (leading, meaningful_bits)
                } else {
                    // Use previous block info
                    let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
                    (self.prev_leading, meaningful_bits)
                };

                let meaningful = self.read_bits(meaningful_bits as usize)?;
                let trailing = 64 - leading - meaningful_bits;
                let xor = meaningful << trailing;
                self.prev_value ^ xor
            }
        };

        self.prev_value = bits;
        self.count += 1;
        Ok(Some(f64::from_bits(bits)))
    }

    /// Decode all values into a vector
    pub fn decode_all(&mut self) -> Result<Vec<f64>> {
        let mut values = Vec::with_capacity(self.expected_count);
        while let Some(value) = self.decode()? {
            values.push(value);
        }
        Ok(values)
    }

    fn read_bit(&mut self) -> Result<bool> {
        if self.byte_idx >= self.data.len() {
            return Err(CompressionError::BufferUnderflow);
        }

        let bit = (self.data[self.byte_idx] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_idx += 1;
        }
        Ok(bit)
    }

    fn read_bits(&mut self, num_bits: usize) -> Result<u64> {
        let mut value = 0u64;
        for _ in 0..num_bits {
            value = (value << 1) | (self.read_bit()? as u64);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_value() {
        let mut encoder = GorillaEncoder::new();
        encoder.encode(42.5);
        let data = encoder.finish();

        let mut decoder = GorillaDecoder::new(&data, 1);
        assert_eq!(decoder.decode().unwrap(), Some(42.5));
        assert_eq!(decoder.decode().unwrap(), None);
    }

    #[test]
    fn test_identical_values() {
        let mut encoder = GorillaEncoder::new();
        for _ in 0..100 {
            encoder.encode(3.14159);
        }
        let data = encoder.finish();

        // Identical values should compress very well
        // First value: 8 bytes, then 1 bit each = ~8 + 100/8 = ~21 bytes
        assert!(data.len() < 30);

        let mut decoder = GorillaDecoder::new(&data, 100);
        let values = decoder.decode_all().unwrap();
        assert_eq!(values.len(), 100);
        for v in values {
            assert!((v - 3.14159).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_similar_values() {
        let mut encoder = GorillaEncoder::new();
        let values: Vec<f64> = (0..100).map(|i| 100.0 + (i as f64) * 0.01).collect();

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        // Similar values should compress somewhat
        // (but small increments can change many bits in IEEE754 representation)
        let compression_ratio = (values.len() * 8) as f64 / data.len() as f64;
        assert!(compression_ratio > 1.0, "Expected some compression, got {}x", compression_ratio);

        let mut decoder = GorillaDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();
        assert_eq!(decoded.len(), values.len());

        for (original, decoded) in values.iter().zip(decoded.iter()) {
            assert!((original - decoded).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_edge_cases() {
        let mut encoder = GorillaEncoder::new();
        let values = vec![
            0.0,
            -0.0,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let mut decoder = GorillaDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();

        assert_eq!(decoded[0].to_bits(), 0.0_f64.to_bits());
        assert_eq!(decoded[1].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(decoded[2], f64::MIN);
        assert_eq!(decoded[3], f64::MAX);
        assert_eq!(decoded[4], f64::MIN_POSITIVE);
        assert_eq!(decoded[5], f64::INFINITY);
        assert_eq!(decoded[6], f64::NEG_INFINITY);
    }

    #[test]
    fn test_nan_values() {
        let mut encoder = GorillaEncoder::new();
        encoder.encode(f64::NAN);
        encoder.encode(1.0);
        encoder.encode(f64::NAN);
        let data = encoder.finish();

        let mut decoder = GorillaDecoder::new(&data, 3);
        let decoded = decoder.decode_all().unwrap();

        assert!(decoded[0].is_nan());
        assert_eq!(decoded[1], 1.0);
        assert!(decoded[2].is_nan());
    }

    #[test]
    fn test_random_values() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let mut encoder = GorillaEncoder::new();
        let values: Vec<f64> = (0..1000).map(|_| rng.gen_range(-1000.0..1000.0)).collect();

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let mut decoder = GorillaDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();

        for (original, decoded) in values.iter().zip(decoded.iter()) {
            assert_eq!(original.to_bits(), decoded.to_bits());
        }
    }

    #[test]
    fn test_compression_ratio() {
        // Test with CPU-like metrics that increase slowly
        let mut encoder = GorillaEncoder::new();
        let values: Vec<f64> = (0..1000)
            .map(|i| 50.0 + (i as f64 * 0.1).sin() * 10.0)
            .collect();

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let uncompressed_size = values.len() * 8;
        let compression_ratio = uncompressed_size as f64 / data.len() as f64;

        // Smooth sinusoidal data should achieve some compression
        // Note: Gorilla works best when consecutive values have similar binary representations
        assert!(
            compression_ratio > 1.0,
            "Expected some compression, got {}x",
            compression_ratio
        );
    }
}
