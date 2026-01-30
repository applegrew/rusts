//! Optimized bit-level I/O operations
//!
//! Provides efficient bit writing and reading by batching operations
//! instead of per-bit function calls. This significantly improves
//! compression/decompression performance.

use crate::error::{CompressionError, Result};

/// Optimized bit writer that batches bit operations
pub struct BitWriter {
    buffer: Vec<u8>,
    /// Current byte being built (accumulator)
    current_byte: u8,
    /// Number of bits in current_byte (0-7)
    bits_in_byte: u8,
}

impl BitWriter {
    /// Create a new BitWriter with default capacity
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
            current_byte: 0,
            bits_in_byte: 0,
        }
    }

    /// Create a new BitWriter with specified capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            current_byte: 0,
            bits_in_byte: 0,
        }
    }

    /// Write a single bit
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bits_in_byte);
        }
        self.bits_in_byte += 1;

        if self.bits_in_byte == 8 {
            self.buffer.push(self.current_byte);
            self.current_byte = 0;
            self.bits_in_byte = 0;
        }
    }

    /// Write multiple bits from a u64 value (optimized batch operation)
    ///
    /// Writes `num_bits` bits from the least significant bits of `value`,
    /// most significant bit first.
    #[inline]
    pub fn write_bits(&mut self, value: u64, num_bits: usize) {
        debug_assert!(num_bits <= 64);

        if num_bits == 0 {
            return;
        }

        let mut remaining_bits = num_bits;
        let mut shift = num_bits;

        // Handle bits that don't fill current byte
        while remaining_bits > 0 {
            let bits_available = 8 - self.bits_in_byte as usize;

            if remaining_bits >= bits_available {
                // Fill the rest of current byte
                shift -= bits_available;
                // Safe mask: when bits_available == 8, mask is 0xFF
                let mask = if bits_available == 8 { 0xFF } else { (1u8 << bits_available) - 1 };
                let bits_to_write = ((value >> shift) as u8) & mask;
                self.current_byte |= bits_to_write;
                self.buffer.push(self.current_byte);
                self.current_byte = 0;
                self.bits_in_byte = 0;
                remaining_bits -= bits_available;
            } else {
                // Partial byte - just update accumulator
                shift -= remaining_bits;
                let mask = (1u8 << remaining_bits) - 1;
                let bits_to_write = ((value >> shift) as u8) & mask;
                self.current_byte |= bits_to_write << (bits_available - remaining_bits);
                self.bits_in_byte += remaining_bits as u8;
                remaining_bits = 0;
            }
        }
    }

    /// Write exactly 64 bits (optimized path for full u64)
    #[inline]
    pub fn write_u64(&mut self, value: u64) {
        if self.bits_in_byte == 0 {
            // Byte-aligned: write directly
            self.buffer.extend_from_slice(&value.to_be_bytes());
        } else {
            // Not aligned: use general path
            self.write_bits(value, 64);
        }
    }

    /// Finish writing and return the buffer
    ///
    /// Any partial byte is included (padded with zeros).
    #[inline]
    pub fn finish(mut self) -> Vec<u8> {
        if self.bits_in_byte > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }

    /// Get current compressed size in bytes (including partial byte)
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len() + if self.bits_in_byte > 0 { 1 } else { 0 }
    }

    /// Check if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.bits_in_byte == 0
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimized bit reader that batches bit operations
pub struct BitReader<'a> {
    data: &'a [u8],
    /// Current byte index
    byte_idx: usize,
    /// Current bit position within byte (0-7)
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new BitReader
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_idx: 0,
            bit_pos: 0,
        }
    }

    /// Read a single bit
    #[inline]
    pub fn read_bit(&mut self) -> Result<bool> {
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

    /// Read multiple bits into a u64 (optimized batch operation)
    ///
    /// Returns bits packed into the least significant bits of the result.
    #[inline]
    pub fn read_bits(&mut self, num_bits: usize) -> Result<u64> {
        debug_assert!(num_bits <= 64);

        if num_bits == 0 {
            return Ok(0);
        }

        let mut value = 0u64;
        let mut remaining_bits = num_bits;

        while remaining_bits > 0 {
            if self.byte_idx >= self.data.len() {
                return Err(CompressionError::BufferUnderflow);
            }

            let bits_available = 8 - self.bit_pos as usize;
            let current_byte = self.data[self.byte_idx];

            if remaining_bits >= bits_available {
                // Take all remaining bits from current byte
                // Safe mask: when bits_available == 8, mask is 0xFF
                let mask = if bits_available == 8 { 0xFF } else { (1u8 << bits_available) - 1 };
                let bits = (current_byte & mask) as u64;
                value = (value << bits_available) | bits;
                remaining_bits -= bits_available;
                self.byte_idx += 1;
                self.bit_pos = 0;
            } else {
                // Take partial bits from current byte
                let shift = bits_available - remaining_bits;
                let mask = ((1u8 << remaining_bits) - 1) << shift;
                let bits = ((current_byte & mask) >> shift) as u64;
                value = (value << remaining_bits) | bits;
                self.bit_pos += remaining_bits as u8;
                remaining_bits = 0;
            }
        }

        Ok(value)
    }

    /// Read exactly 64 bits (optimized path for full u64)
    #[inline]
    pub fn read_u64(&mut self) -> Result<u64> {
        if self.bit_pos == 0 && self.byte_idx + 8 <= self.data.len() {
            // Byte-aligned: read directly
            let bytes: [u8; 8] = self.data[self.byte_idx..self.byte_idx + 8]
                .try_into()
                .unwrap();
            self.byte_idx += 8;
            Ok(u64::from_be_bytes(bytes))
        } else {
            // Not aligned: use general path
            self.read_bits(64)
        }
    }

    /// Check if there are more bits available
    #[inline]
    pub fn has_more(&self) -> bool {
        self.byte_idx < self.data.len()
    }

    /// Get current position in bits
    #[inline]
    pub fn position_bits(&self) -> usize {
        self.byte_idx * 8 + self.bit_pos as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read_single_bits() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bit(false);
        let data = writer.finish();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0], 0b10110010);

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bit().unwrap(), true);
        assert_eq!(reader.read_bit().unwrap(), false);
        assert_eq!(reader.read_bit().unwrap(), true);
        assert_eq!(reader.read_bit().unwrap(), true);
        assert_eq!(reader.read_bit().unwrap(), false);
        assert_eq!(reader.read_bit().unwrap(), false);
        assert_eq!(reader.read_bit().unwrap(), true);
        assert_eq!(reader.read_bit().unwrap(), false);
    }

    #[test]
    fn test_write_read_bits() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b10110, 5);
        writer.write_bits(0b111, 3);
        let data = writer.finish();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0], 0b10110111);

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(5).unwrap(), 0b10110);
        assert_eq!(reader.read_bits(3).unwrap(), 0b111);
    }

    #[test]
    fn test_write_read_u64() {
        let mut writer = BitWriter::new();
        writer.write_u64(0x123456789ABCDEF0);
        let data = writer.finish();

        assert_eq!(data.len(), 8);

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_write_read_mixed() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bits(0b10110, 5);
        writer.write_u64(0xDEADBEEF);
        writer.write_bits(0b11, 2);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bit().unwrap(), true);
        assert_eq!(reader.read_bits(5).unwrap(), 0b10110);
        assert_eq!(reader.read_bits(64).unwrap(), 0xDEADBEEF);
        assert_eq!(reader.read_bits(2).unwrap(), 0b11);
    }

    #[test]
    fn test_cross_byte_boundary() {
        let mut writer = BitWriter::new();
        // Write 12 bits across byte boundary
        writer.write_bits(0b101011001110, 12);
        let data = writer.finish();

        assert_eq!(data.len(), 2);
        // First byte: 10101100, second byte: 1110xxxx
        assert_eq!(data[0], 0b10101100);
        assert_eq!(data[1] & 0xF0, 0b11100000);

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(12).unwrap(), 0b101011001110);
    }

    #[test]
    fn test_large_values() {
        let mut writer = BitWriter::new();
        writer.write_bits(u64::MAX, 64);
        writer.write_bits(0, 64);
        writer.write_bits(0x5555555555555555, 64);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(64).unwrap(), u64::MAX);
        assert_eq!(reader.read_bits(64).unwrap(), 0);
        assert_eq!(reader.read_bits(64).unwrap(), 0x5555555555555555);
    }

    #[test]
    fn test_various_bit_counts() {
        for num_bits in 1..=64 {
            // All 1s for given bit count (handle 64-bit case to avoid overflow)
            let value = if num_bits == 64 { u64::MAX } else { (1u64 << num_bits) - 1 };
            let mut writer = BitWriter::new();
            writer.write_bits(value, num_bits);
            let data = writer.finish();

            let mut reader = BitReader::new(&data);
            assert_eq!(reader.read_bits(num_bits).unwrap(), value, "Failed for {} bits", num_bits);
        }
    }
}
