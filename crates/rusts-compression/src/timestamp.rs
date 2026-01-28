//! Delta-of-delta timestamp compression
//!
//! Implements timestamp compression using delta-of-delta encoding.
//! This works well for time series data where timestamps are typically
//! evenly spaced (e.g., every second or every 10 seconds).
//!
//! The algorithm:
//! 1. First timestamp: stored as-is (64 bits)
//! 2. Second timestamp: store delta from first
//! 3. Subsequent timestamps: store delta-of-delta (DoD)
//!
//! DoD encoding:
//! - If DoD == 0: single 0 bit
//! - If |DoD| <= 63: '10' + 7 bits (sign + value)
//! - If |DoD| <= 255: '110' + 9 bits
//! - If |DoD| <= 2047: '1110' + 12 bits
//! - Otherwise: '1111' + 64 bits

use crate::error::{CompressionError, Result};
use rusts_core::Timestamp;

/// Delta-of-delta timestamp encoder
pub struct TimestampEncoder {
    buffer: Vec<u8>,
    bit_pos: u8,
    prev_timestamp: Timestamp,
    prev_delta: i64,
    count: usize,
}

impl TimestampEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(256),
            bit_pos: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            count: 0,
        }
    }

    /// Create encoder with capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            bit_pos: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            count: 0,
        }
    }

    /// Encode a timestamp
    pub fn encode(&mut self, timestamp: Timestamp) {
        if self.count == 0 {
            // First timestamp: store full 64 bits
            self.write_bits(timestamp as u64, 64);
            self.prev_timestamp = timestamp;
            self.count = 1;
            return;
        }

        let delta = timestamp.wrapping_sub(self.prev_timestamp);

        if self.count == 1 {
            // Second timestamp: store delta
            // Use variable-length encoding for the first delta
            self.encode_delta(delta);
            self.prev_timestamp = timestamp;
            self.prev_delta = delta;
            self.count = 2;
            return;
        }

        // Subsequent timestamps: store delta-of-delta
        let dod = delta.wrapping_sub(self.prev_delta);
        self.encode_dod(dod);

        self.prev_timestamp = timestamp;
        self.prev_delta = delta;
        self.count += 1;
    }

    /// Finish encoding and return compressed bytes
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    /// Get the number of timestamps encoded
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get current compressed size
    pub fn compressed_size(&self) -> usize {
        self.buffer.len()
    }

    fn encode_delta(&mut self, delta: i64) {
        // Variable-length encoding for first delta
        if delta == 0 {
            self.write_bit(false);
        } else if delta >= -63 && delta <= 64 {
            self.write_bits(0b10, 2);
            self.write_bits(((delta + 63) as u64) & 0x7F, 7);
        } else if delta >= -255 && delta <= 256 {
            self.write_bits(0b110, 3);
            self.write_bits(((delta + 255) as u64) & 0x1FF, 9);
        } else if delta >= -2047 && delta <= 2048 {
            self.write_bits(0b1110, 4);
            self.write_bits(((delta + 2047) as u64) & 0xFFF, 12);
        } else {
            self.write_bits(0b1111, 4);
            self.write_bits(delta as u64, 64);
        }
    }

    fn encode_dod(&mut self, dod: i64) {
        if dod == 0 {
            // Single 0 bit for zero delta-of-delta
            self.write_bit(false);
        } else if dod >= -63 && dod <= 64 {
            // '10' prefix + 7 bits
            self.write_bits(0b10, 2);
            self.write_bits(((dod + 63) as u64) & 0x7F, 7);
        } else if dod >= -255 && dod <= 256 {
            // '110' prefix + 9 bits
            self.write_bits(0b110, 3);
            self.write_bits(((dod + 255) as u64) & 0x1FF, 9);
        } else if dod >= -2047 && dod <= 2048 {
            // '1110' prefix + 12 bits
            self.write_bits(0b1110, 4);
            self.write_bits(((dod + 2047) as u64) & 0xFFF, 12);
        } else {
            // '1111' prefix + 64 bits
            self.write_bits(0b1111, 4);
            self.write_bits(dod as u64, 64);
        }
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

impl Default for TimestampEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Delta-of-delta timestamp decoder
pub struct TimestampDecoder<'a> {
    data: &'a [u8],
    byte_idx: usize,
    bit_pos: u8,
    prev_timestamp: Timestamp,
    prev_delta: i64,
    count: usize,
    expected_count: usize,
}

impl<'a> TimestampDecoder<'a> {
    /// Create a new decoder
    pub fn new(data: &'a [u8], expected_count: usize) -> Self {
        Self {
            data,
            byte_idx: 0,
            bit_pos: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            count: 0,
            expected_count,
        }
    }

    /// Decode the next timestamp
    pub fn decode(&mut self) -> Result<Option<Timestamp>> {
        if self.count >= self.expected_count {
            return Ok(None);
        }

        let timestamp = if self.count == 0 {
            // First timestamp: read 64 bits
            self.read_bits(64)? as Timestamp
        } else if self.count == 1 {
            // Second timestamp: read delta
            let delta = self.decode_delta()?;
            self.prev_delta = delta;
            self.prev_timestamp.wrapping_add(delta)
        } else {
            // Subsequent: read delta-of-delta
            let dod = self.decode_dod()?;
            let delta = self.prev_delta.wrapping_add(dod);
            self.prev_delta = delta;
            self.prev_timestamp.wrapping_add(delta)
        };

        self.prev_timestamp = timestamp;
        self.count += 1;
        Ok(Some(timestamp))
    }

    /// Decode all timestamps
    pub fn decode_all(&mut self) -> Result<Vec<Timestamp>> {
        let mut timestamps = Vec::with_capacity(self.expected_count);
        while let Some(ts) = self.decode()? {
            timestamps.push(ts);
        }
        Ok(timestamps)
    }

    fn decode_delta(&mut self) -> Result<i64> {
        if !self.read_bit()? {
            return Ok(0);
        }

        if !self.read_bit()? {
            // '10' prefix
            let val = self.read_bits(7)?;
            return Ok(val as i64 - 63);
        }

        if !self.read_bit()? {
            // '110' prefix
            let val = self.read_bits(9)?;
            return Ok(val as i64 - 255);
        }

        if !self.read_bit()? {
            // '1110' prefix
            let val = self.read_bits(12)?;
            return Ok(val as i64 - 2047);
        }

        // '1111' prefix - full 64 bits
        Ok(self.read_bits(64)? as i64)
    }

    fn decode_dod(&mut self) -> Result<i64> {
        if !self.read_bit()? {
            return Ok(0);
        }

        if !self.read_bit()? {
            // '10' prefix
            let val = self.read_bits(7)?;
            return Ok(val as i64 - 63);
        }

        if !self.read_bit()? {
            // '110' prefix
            let val = self.read_bits(9)?;
            return Ok(val as i64 - 255);
        }

        if !self.read_bit()? {
            // '1110' prefix
            let val = self.read_bits(12)?;
            return Ok(val as i64 - 2047);
        }

        // '1111' prefix - full 64 bits
        Ok(self.read_bits(64)? as i64)
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
    fn test_single_timestamp() {
        let mut encoder = TimestampEncoder::new();
        encoder.encode(1609459200000000000);
        let data = encoder.finish();

        let mut decoder = TimestampDecoder::new(&data, 1);
        assert_eq!(decoder.decode().unwrap(), Some(1609459200000000000));
        assert_eq!(decoder.decode().unwrap(), None);
    }

    #[test]
    fn test_evenly_spaced_timestamps() {
        let mut encoder = TimestampEncoder::new();
        let base = 1609459200000000000_i64;
        let interval = 1_000_000_000_i64; // 1 second in nanos

        // 1000 evenly-spaced timestamps
        for i in 0..1000 {
            encoder.encode(base + i * interval);
        }
        let data = encoder.finish();

        // Should compress extremely well (mostly zero DoDs)
        let uncompressed_size = 1000 * 8;
        let compression_ratio = uncompressed_size as f64 / data.len() as f64;
        assert!(
            compression_ratio > 10.0,
            "Expected >10x compression, got {}x",
            compression_ratio
        );

        let mut decoder = TimestampDecoder::new(&data, 1000);
        let decoded = decoder.decode_all().unwrap();
        assert_eq!(decoded.len(), 1000);

        for (i, &ts) in decoded.iter().enumerate() {
            assert_eq!(ts, base + i as i64 * interval);
        }
    }

    #[test]
    fn test_slightly_irregular_timestamps() {
        let mut encoder = TimestampEncoder::new();
        let base = 1609459200000000000_i64;
        let interval = 1_000_000_000_i64;

        // Add some jitter
        let timestamps: Vec<i64> = (0..1000)
            .map(|i| base + i * interval + (i % 10) * 1000)
            .collect();

        for &ts in &timestamps {
            encoder.encode(ts);
        }
        let data = encoder.finish();

        let mut decoder = TimestampDecoder::new(&data, timestamps.len());
        let decoded = decoder.decode_all().unwrap();

        for (original, decoded) in timestamps.iter().zip(decoded.iter()) {
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_large_gaps() {
        let mut encoder = TimestampEncoder::new();
        let timestamps = vec![
            1609459200000000000_i64,
            1609459200000000000 + 1_000_000_000,       // 1s later
            1609459200000000000 + 86400_000_000_000,   // 1 day later
            1609459200000000000 + 86401_000_000_000,   // 1s after that
            1609459200000000000 + 86402_000_000_000,   // 1s after that
        ];

        for &ts in &timestamps {
            encoder.encode(ts);
        }
        let data = encoder.finish();

        let mut decoder = TimestampDecoder::new(&data, timestamps.len());
        let decoded = decoder.decode_all().unwrap();

        for (original, decoded) in timestamps.iter().zip(decoded.iter()) {
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_zero_and_negative() {
        let mut encoder = TimestampEncoder::new();
        let timestamps = vec![0_i64, 1, 2, 3, -1000, -999, -998];

        for &ts in &timestamps {
            encoder.encode(ts);
        }
        let data = encoder.finish();

        let mut decoder = TimestampDecoder::new(&data, timestamps.len());
        let decoded = decoder.decode_all().unwrap();

        for (original, decoded) in timestamps.iter().zip(decoded.iter()) {
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_extreme_values() {
        let mut encoder = TimestampEncoder::new();
        let timestamps = vec![i64::MIN, 0, i64::MAX];

        for &ts in &timestamps {
            encoder.encode(ts);
        }
        let data = encoder.finish();

        let mut decoder = TimestampDecoder::new(&data, timestamps.len());
        let decoded = decoder.decode_all().unwrap();

        for (original, decoded) in timestamps.iter().zip(decoded.iter()) {
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_identical_timestamps() {
        let mut encoder = TimestampEncoder::new();
        let ts = 1609459200000000000_i64;

        for _ in 0..100 {
            encoder.encode(ts);
        }
        let data = encoder.finish();

        // 100 identical timestamps should compress well
        // First: 8 bytes, second: delta=0 (1 bit), rest: DoD=0 (1 bit each)
        // Total bits: 64 + 1 + 98 = 163 bits = ~21 bytes + overhead
        assert!(data.len() < 30, "Expected < 30 bytes, got {}", data.len());

        let mut decoder = TimestampDecoder::new(&data, 100);
        let decoded = decoder.decode_all().unwrap();

        for &decoded_ts in &decoded {
            assert_eq!(decoded_ts, ts);
        }
    }
}
