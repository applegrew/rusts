//! HTTP writer for sending data to RusTs server

use crate::error::{ImportError, Result};
use rusts_core::Point;
use std::time::Duration;

/// HTTP client for writing to RusTs server
pub struct RustsWriter {
    client: reqwest::Client,
    base_url: String,
}

impl RustsWriter {
    /// Create a new writer connecting to the given server URL
    pub fn new(server_url: impl Into<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let mut base_url = server_url.into();
        // Remove trailing slash
        if base_url.ends_with('/') {
            base_url.pop();
        }

        Ok(Self { client, base_url })
    }

    /// Check if the server is healthy
    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);
        let response = self.client.get(&url).send().await?;
        Ok(response.status().is_success())
    }

    /// Write points to the server using line protocol
    pub async fn write(&self, points: &[Point]) -> Result<WriteResult> {
        if points.is_empty() {
            return Ok(WriteResult {
                points_written: 0,
                bytes_sent: 0,
            });
        }

        // Convert points to line protocol
        let line_protocol = points_to_line_protocol(points);
        let bytes_sent = line_protocol.len();

        let url = format!("{}/write", self.base_url);
        let response = self.client
            .post(&url)
            .body(line_protocol)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(WriteResult {
                points_written: points.len(),
                bytes_sent,
            })
        } else {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            Err(ImportError::Server { status, message })
        }
    }

    /// Write points in batches
    pub async fn write_batched(
        &self,
        points: &[Point],
        batch_size: usize,
    ) -> Result<WriteResult> {
        self.write_batched_with_progress(points, batch_size, |_, _| {}).await
    }

    /// Write points in batches with progress callback
    ///
    /// The callback receives (points_written_so_far, total_points) after each batch.
    pub async fn write_batched_with_progress<F>(
        &self,
        points: &[Point],
        batch_size: usize,
        mut on_progress: F,
    ) -> Result<WriteResult>
    where
        F: FnMut(usize, usize),
    {
        let mut total_written = 0;
        let mut total_bytes = 0;
        let total_points = points.len();

        for batch in points.chunks(batch_size) {
            let result = self.write(batch).await?;
            total_written += result.points_written;
            total_bytes += result.bytes_sent;
            on_progress(total_written, total_points);
        }

        Ok(WriteResult {
            points_written: total_written,
            bytes_sent: total_bytes,
        })
    }
}

/// Result of a write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of points written
    pub points_written: usize,
    /// Bytes sent
    pub bytes_sent: usize,
}

/// Convert points to InfluxDB line protocol format
fn points_to_line_protocol(points: &[Point]) -> String {
    let mut lines = Vec::with_capacity(points.len());

    for point in points {
        let mut line = point.measurement.clone();

        // Add tags
        if !point.tags.is_empty() {
            for tag in &point.tags {
                line.push(',');
                line.push_str(&escape_tag_key(&tag.key));
                line.push('=');
                line.push_str(&escape_tag_value(&tag.value));
            }
        }

        // Add fields
        line.push(' ');
        let fields: Vec<String> = point
            .fields
            .iter()
            .map(|f| {
                let value = match &f.value {
                    rusts_core::FieldValue::Float(v) => format!("{}", v),
                    rusts_core::FieldValue::Integer(v) => format!("{}i", v),
                    rusts_core::FieldValue::UnsignedInteger(v) => format!("{}u", v),
                    rusts_core::FieldValue::String(v) => format!("\"{}\"", escape_string_value(v)),
                    rusts_core::FieldValue::Boolean(v) => format!("{}", v),
                };
                format!("{}={}", escape_field_key(&f.key), value)
            })
            .collect();
        line.push_str(&fields.join(","));

        // Add timestamp
        line.push(' ');
        line.push_str(&point.timestamp.to_string());

        lines.push(line);
    }

    lines.join("\n")
}

/// Escape special characters in tag keys
fn escape_tag_key(s: &str) -> String {
    s.replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

/// Escape special characters in tag values
fn escape_tag_value(s: &str) -> String {
    s.replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

/// Escape special characters in field keys
fn escape_field_key(s: &str) -> String {
    s.replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

/// Escape special characters in string field values
fn escape_string_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::{Field, FieldValue, Tag};

    #[test]
    fn test_points_to_line_protocol() {
        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: vec![
                    Tag {
                        key: "host".to_string(),
                        value: "server01".to_string(),
                    },
                ],
                fields: vec![
                    Field {
                        key: "usage".to_string(),
                        value: FieldValue::Float(64.5),
                    },
                ],
                timestamp: 1609459200000000000,
            },
        ];

        let line = points_to_line_protocol(&points);
        assert_eq!(line, "cpu,host=server01 usage=64.5 1609459200000000000");
    }

    #[test]
    fn test_multiple_fields() {
        let points = vec![
            Point {
                measurement: "system".to_string(),
                tags: vec![],
                fields: vec![
                    Field {
                        key: "cpu".to_string(),
                        value: FieldValue::Float(45.0),
                    },
                    Field {
                        key: "mem".to_string(),
                        value: FieldValue::Integer(8192),
                    },
                ],
                timestamp: 1000000000,
            },
        ];

        let line = points_to_line_protocol(&points);
        assert!(line.contains("cpu=45"));
        assert!(line.contains("mem=8192i"));
    }

    #[test]
    fn test_escape_special_chars() {
        assert_eq!(escape_tag_key("host,name"), "host\\,name");
        assert_eq!(escape_tag_value("us west"), "us\\ west");
        assert_eq!(escape_string_value("hello \"world\""), "hello \\\"world\\\"");
    }
}
