# RusTs Compression - Architecture

## Overview

The `rusts-compression` crate implements specialized compression algorithms optimized for time series data. It provides multiple compression strategies for different data types, achieving 10-15x compression ratios in typical deployments.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── error.rs        # Error types
├── float.rs        # Gorilla XOR compression for floats
├── timestamp.rs    # Delta-of-delta encoding for timestamps
├── integer.rs      # Integer compression with zigzag encoding
├── dictionary.rs   # Dictionary encoding for strings
└── block.rs        # Block compression (LZ4/Zstd)
```

## Compression Algorithms

### 1. Gorilla XOR Compression (float.rs)

**Algorithm:** Facebook's Gorilla algorithm for floating-point time series.

**Reference:** "Gorilla: A Fast, Scalable, In-Memory Time Series Database"

#### GorillaEncoder (float.rs:13-26)

```rust
pub struct GorillaEncoder {
    buffer: Vec<u8>,
    bit_pos: u8,
    prev_value: u64,
    prev_leading: u8,
    prev_trailing: u8,
    count: usize,
}
```

**Encoding Logic (float.rs:54-105):**
1. **First value:** Store full 64 bits uncompressed
2. **Identical values:** Write single 0 bit
3. **Different values:**
   - XOR current with previous
   - Calculate leading zeros (capped at 31) and trailing zeros
   - If leading >= prev_leading AND trailing >= prev_trailing:
     - Write 0 bit + store only meaningful bits
   - Else:
     - Write 1 bit + 5 bits for leading + 6 bits for length + meaningful bits

**Typical Compression:** 8-12x for smooth, gradually changing data

#### GorillaDecoder (float.rs:153-237)

Reverse process with state tracking for leading/trailing zeros.

### 2. Delta-of-Delta Timestamp Compression (timestamp.rs)

**Algorithm:** Variable-length encoding optimized for evenly-spaced timestamps.

#### Encoding Scheme (timestamp.rs:12-17)

| Condition | Encoding |
|-----------|----------|
| DoD == 0 | Single 0 bit |
| \|DoD\| <= 63 | '10' prefix + 7 bits (sign + value) |
| \|DoD\| <= 255 | '110' prefix + 9 bits |
| \|DoD\| <= 2047 | '1110' prefix + 12 bits |
| Otherwise | '1111' prefix + 64 bits |

#### TimestampEncoder (timestamp.rs:23-98)

```rust
pub struct TimestampEncoder {
    buffer: Vec<u8>,
    bit_pos: u8,
    prev_timestamp: Timestamp,
    prev_delta: i64,
    count: usize,
}
```

**Encoding Process:**
1. First timestamp: Store full 64 bits
2. Second timestamp: Store delta from first
3. Subsequent: Store delta-of-delta (DoD = delta[i] - delta[i-1])

**Typical Compression:** 10-15x for regular intervals

**Example:** 1000 timestamps at 1-second intervals compress from 8KB to <800 bytes

### 3. Integer Compression (integer.rs)

**Algorithm:** Delta encoding + zigzag encoding + variable-length coding.

#### Zigzag Encoding (integer.rs:165-176)

Maps signed integers to unsigned for efficient varint encoding:
- 0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4...
- Formula: `((value << 1) ^ (value >> 63))`

#### Variable-Length Encoding (integer.rs:67-78)

Each byte: 7 bits data + 1 continuation bit (MSB)
- Values 0-127: 1 byte
- Values 0-16383: 2 bytes
- And so on...

#### IntegerEncoder (integer.rs:9-65)

```rust
pub struct IntegerEncoder {
    buffer: Vec<u8>,
    prev_value: i64,
    count: usize,
}
```

**Typical Compression:** 4-8x for sequential values

### 4. Dictionary Encoding (dictionary.rs)

**Algorithm:** Maps strings to integer IDs for low-cardinality values.

#### DictionaryEncoder (dictionary.rs:10-109)

```rust
pub struct DictionaryEncoder {
    string_to_id: HashMap<String, u32>,
    id_to_string: Vec<String>,
    encoded: Vec<u32>,
    max_size: usize,  // Default: 65536
}
```

**Serialization Format (dictionary.rs:87-109):**
```
[dict_size: u32]
[str_len: u32, str_bytes: [u8]]* (repeated for each entry)
[values_count: u32]
[id: u32]* (repeated for each encoded value)
```

**Typical Compression:** 3-10x for repeated string values

**Use Case:** Tag values, hostnames, regions, environments

### 5. Block Compression (block.rs)

**Purpose:** Additional compression on top of specialized encoders.

#### CompressionLevel (block.rs:11-24)

```rust
pub enum CompressionLevel {
    None,       // No compression
    Fast,       // LZ4 fast mode
    Default,    // LZ4 high compression
    Balanced,   // Zstd level 1
    High,       // Zstd level 3 (default)
    Best,       // Zstd level 9
}
```

#### Algorithm Mapping

| Level | Algorithm | Use Case |
|-------|-----------|----------|
| Fast | LZ4 | Hot data, low latency |
| Default | LZ4 HC (level 9) | Balanced |
| Balanced | Zstd level 1 | Warm data |
| High | Zstd level 3 | General purpose |
| Best | Zstd level 9 | Cold data, storage optimization |

#### BlockCompressor (block.rs:72-130)

**Compression Format:**
- LZ4: `magic_byte(1) + original_size(u32, LE) + compressed_data`
- Zstd: `magic_byte(1) + compressed_data` (size auto-included)

**Magic Bytes:**
- None: 0x00
- LZ4: 0x01
- Zstd: 0x02

## Error Handling (error.rs)

```rust
pub enum CompressionError {
    BufferOverflow,
    BufferUnderflow,
    InvalidData,
    DictionaryFull,
    InvalidDictionaryIndex,
    CompressionFailed,
    DecompressionFailed,
    Io(std::io::Error),
}
```

## Key Design Patterns

### Bit-Level Operations
Gorilla and Timestamp encoders work at bit granularity using:
- `write_bit()` and `write_bits()` for efficient packing
- `bit_pos` tracking (0-7) within bytes

### Stateful Encoding
Each encoder maintains history:
- Gorilla: Previous value, leading/trailing zeros
- Timestamp: Previous timestamp and delta
- Integer: Previous value for delta encoding

### Capacity Planning
- Encoders support `with_capacity()` for pre-allocation
- Dictionary has configurable max size (default 65536)

## Compression Strategy Selection

| Data Type | Algorithm | Typical Ratio |
|-----------|-----------|---------------|
| Timestamps | Delta-of-delta | 10-15x |
| Floats | Gorilla XOR | 8-12x |
| Integers | Zigzag + Varint | 4-8x |
| Strings | Dictionary | 3-10x |

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Core types (Timestamp, FieldValue) |
| `lz4` | LZ4 compression |
| `zstd` | Zstandard compression |
| `thiserror` | Error handling |
| `bytes` | Byte buffer utilities |
| `serde` | Serialization |

## Integration Points

Used by:
- `rusts-storage` - Segment compression during flush
- `rusts-importer` - Direct write mode compression settings
