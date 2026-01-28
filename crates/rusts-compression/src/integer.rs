//! Integer compression using delta encoding with variable-length coding
//!
//! Uses delta encoding followed by zigzag encoding to handle signed deltas,
//! then variable-length encoding for efficient storage.

use crate::error::{CompressionError, Result};

/// Integer encoder using delta + variable-length encoding
pub struct IntegerEncoder {
    buffer: Vec<u8>,
    prev_value: i64,
    count: usize,
}

impl IntegerEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(256),
            prev_value: 0,
            count: 0,
        }
    }

    /// Create encoder with capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            prev_value: 0,
            count: 0,
        }
    }

    /// Encode an integer value
    pub fn encode(&mut self, value: i64) {
        if self.count == 0 {
            // First value: store using zigzag + varint
            self.write_varint(zigzag_encode(value));
            self.prev_value = value;
            self.count = 1;
            return;
        }

        // Delta from previous value (use wrapping to handle overflow)
        let delta = value.wrapping_sub(self.prev_value);
        self.write_varint(zigzag_encode(delta));

        self.prev_value = value;
        self.count += 1;
    }

    /// Finish encoding and return compressed bytes
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    /// Get count of encoded values
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get current compressed size
    pub fn compressed_size(&self) -> usize {
        self.buffer.len()
    }

    fn write_varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                self.buffer.push(byte);
                break;
            } else {
                self.buffer.push(byte | 0x80);
            }
        }
    }
}

impl Default for IntegerEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Integer decoder
pub struct IntegerDecoder<'a> {
    data: &'a [u8],
    pos: usize,
    prev_value: i64,
    count: usize,
    expected_count: usize,
}

impl<'a> IntegerDecoder<'a> {
    /// Create a new decoder
    pub fn new(data: &'a [u8], expected_count: usize) -> Self {
        Self {
            data,
            pos: 0,
            prev_value: 0,
            count: 0,
            expected_count,
        }
    }

    /// Decode the next value
    pub fn decode(&mut self) -> Result<Option<i64>> {
        if self.count >= self.expected_count {
            return Ok(None);
        }

        let encoded = self.read_varint()?;
        let decoded = zigzag_decode(encoded);

        let value = if self.count == 0 {
            decoded
        } else {
            self.prev_value.wrapping_add(decoded)
        };

        self.prev_value = value;
        self.count += 1;
        Ok(Some(value))
    }

    /// Decode all values
    pub fn decode_all(&mut self) -> Result<Vec<i64>> {
        let mut values = Vec::with_capacity(self.expected_count);
        while let Some(v) = self.decode()? {
            values.push(v);
        }
        Ok(values)
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            if self.pos >= self.data.len() {
                return Err(CompressionError::BufferUnderflow);
            }

            let byte = self.data[self.pos];
            self.pos += 1;

            value |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 64 {
                return Err(CompressionError::InvalidData("varint overflow".to_string()));
            }
        }

        Ok(value)
    }
}

/// Zigzag encode a signed integer to unsigned
/// Maps: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
#[inline]
fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Zigzag decode an unsigned integer to signed
#[inline]
fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

/// Unsigned integer encoder
pub struct UnsignedIntegerEncoder {
    buffer: Vec<u8>,
    prev_value: u64,
    count: usize,
}

impl UnsignedIntegerEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(256),
            prev_value: 0,
            count: 0,
        }
    }

    /// Encode an unsigned integer
    pub fn encode(&mut self, value: u64) {
        if self.count == 0 {
            self.write_varint(value);
            self.prev_value = value;
            self.count = 1;
            return;
        }

        // Delta (can be negative, use zigzag)
        let delta = value as i64 - self.prev_value as i64;
        self.write_varint(zigzag_encode(delta));

        self.prev_value = value;
        self.count += 1;
    }

    /// Finish encoding
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    /// Get count
    pub fn count(&self) -> usize {
        self.count
    }

    fn write_varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                self.buffer.push(byte);
                break;
            } else {
                self.buffer.push(byte | 0x80);
            }
        }
    }
}

impl Default for UnsignedIntegerEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Unsigned integer decoder
pub struct UnsignedIntegerDecoder<'a> {
    data: &'a [u8],
    pos: usize,
    prev_value: u64,
    count: usize,
    expected_count: usize,
}

impl<'a> UnsignedIntegerDecoder<'a> {
    /// Create a new decoder
    pub fn new(data: &'a [u8], expected_count: usize) -> Self {
        Self {
            data,
            pos: 0,
            prev_value: 0,
            count: 0,
            expected_count,
        }
    }

    /// Decode the next value
    pub fn decode(&mut self) -> Result<Option<u64>> {
        if self.count >= self.expected_count {
            return Ok(None);
        }

        let value = if self.count == 0 {
            self.read_varint()?
        } else {
            let encoded = self.read_varint()?;
            let delta = zigzag_decode(encoded);
            (self.prev_value as i64 + delta) as u64
        };

        self.prev_value = value;
        self.count += 1;
        Ok(Some(value))
    }

    /// Decode all values
    pub fn decode_all(&mut self) -> Result<Vec<u64>> {
        let mut values = Vec::with_capacity(self.expected_count);
        while let Some(v) = self.decode()? {
            values.push(v);
        }
        Ok(values)
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            if self.pos >= self.data.len() {
                return Err(CompressionError::BufferUnderflow);
            }

            let byte = self.data[self.pos];
            self.pos += 1;

            value |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 64 {
                return Err(CompressionError::InvalidData("varint overflow".to_string()));
            }
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);

        // Roundtrip
        for v in [0, 1, -1, 100, -100, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_single_value() {
        let mut encoder = IntegerEncoder::new();
        encoder.encode(42);
        let data = encoder.finish();

        let mut decoder = IntegerDecoder::new(&data, 1);
        assert_eq!(decoder.decode().unwrap(), Some(42));
        assert_eq!(decoder.decode().unwrap(), None);
    }

    #[test]
    fn test_sequential_values() {
        let mut encoder = IntegerEncoder::new();
        for i in 0..1000 {
            encoder.encode(i);
        }
        let data = encoder.finish();

        // Sequential values should compress well (all deltas are 1)
        let compression_ratio = (1000 * 8) as f64 / data.len() as f64;
        assert!(
            compression_ratio > 5.0,
            "Expected >5x compression, got {}x",
            compression_ratio
        );

        let mut decoder = IntegerDecoder::new(&data, 1000);
        let decoded = decoder.decode_all().unwrap();

        for (i, &v) in decoded.iter().enumerate() {
            assert_eq!(v, i as i64);
        }
    }

    #[test]
    fn test_identical_values() {
        let mut encoder = IntegerEncoder::new();
        for _ in 0..1000 {
            encoder.encode(42);
        }
        let data = encoder.finish();

        // All deltas are 0, should compress well
        // First value uses zigzag(42)=84, then 999 zeros (1 byte each)
        assert!(data.len() < 1100, "Expected < 1100 bytes, got {}", data.len());

        let mut decoder = IntegerDecoder::new(&data, 1000);
        let decoded = decoder.decode_all().unwrap();

        for &v in &decoded {
            assert_eq!(v, 42);
        }
    }

    #[test]
    fn test_negative_values() {
        let mut encoder = IntegerEncoder::new();
        let values = vec![-100, -50, 0, 50, 100, -200, -150, -100];

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let mut decoder = IntegerDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_extreme_values() {
        let mut encoder = IntegerEncoder::new();
        let values = vec![i64::MIN, 0, i64::MAX, i64::MIN, i64::MAX];

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let mut decoder = IntegerDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_unsigned_encoder() {
        let mut encoder = UnsignedIntegerEncoder::new();
        let values: Vec<u64> = vec![0, 1, 100, 1000, u64::MAX, 0];

        for &v in &values {
            encoder.encode(v);
        }
        let data = encoder.finish();

        let mut decoder = UnsignedIntegerDecoder::new(&data, values.len());
        let decoded = decoder.decode_all().unwrap();

        assert_eq!(decoded, values);
    }
}
