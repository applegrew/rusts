//! QueryResult to PostgreSQL DataRow encoding

use crate::types::{
    default_field_format, field_value_to_pg_type, field_value_to_string, timestamp_pg_type,
    timestamp_to_string,
};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use rusts_query::model::ResultRow;
use rusts_query::QueryResult;
use std::collections::HashMap;
use std::sync::Arc;

/// Build the field schema for a query result
///
/// Returns a vector of FieldInfo describing each column in the result.
/// The schema is: time (if present), tags (sorted by key), fields (sorted by key).
pub fn build_result_schema(result: &QueryResult) -> Arc<Vec<FieldInfo>> {
    let mut schema = Vec::new();

    // Check if any row has a timestamp
    let has_time = result.rows.iter().any(|r| r.timestamp.is_some());

    if has_time {
        schema.push(FieldInfo::new(
            "time".to_string(),
            None,
            None,
            timestamp_pg_type(),
            default_field_format(),
        ));
    }

    // Collect all unique tag keys from all rows
    let mut tag_keys: Vec<String> = result
        .rows
        .iter()
        .flat_map(|r| r.tags.iter().map(|t| t.key.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    tag_keys.sort();

    for key in &tag_keys {
        schema.push(FieldInfo::new(
            key.clone(),
            None,
            None,
            Type::TEXT,
            default_field_format(),
        ));
    }

    // Collect all unique field keys from all rows and their types
    let mut field_types: HashMap<String, Type> = HashMap::new();
    for row in &result.rows {
        for (key, value) in &row.fields {
            field_types
                .entry(key.clone())
                .or_insert_with(|| field_value_to_pg_type(value));
        }
    }

    let mut field_keys: Vec<String> = field_types.keys().cloned().collect();
    field_keys.sort();

    for key in &field_keys {
        let pg_type = field_types.get(key).cloned().unwrap_or(Type::TEXT);
        schema.push(FieldInfo::new(
            key.clone(),
            None,
            None,
            pg_type,
            default_field_format(),
        ));
    }

    Arc::new(schema)
}

/// Schema metadata for encoding rows
pub struct RowSchema {
    pub has_time: bool,
    pub tag_keys: Vec<String>,
    pub field_keys: Vec<String>,
}

impl RowSchema {
    /// Build schema metadata from a QueryResult
    pub fn from_result(result: &QueryResult) -> Self {
        let has_time = result.rows.iter().any(|r| r.timestamp.is_some());

        let mut tag_keys: Vec<String> = result
            .rows
            .iter()
            .flat_map(|r| r.tags.iter().map(|t| t.key.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        tag_keys.sort();

        let mut field_keys: Vec<String> = result
            .rows
            .iter()
            .flat_map(|r| r.fields.keys().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        field_keys.sort();

        Self {
            has_time,
            tag_keys,
            field_keys,
        }
    }
}

/// Encode a single ResultRow into a PostgreSQL DataRow
pub fn encode_row(
    row: &ResultRow,
    schema: &Arc<Vec<FieldInfo>>,
    row_schema: &RowSchema,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());

    // Encode timestamp if present in schema
    if row_schema.has_time {
        if let Some(ts) = row.timestamp {
            encoder.encode_field_with_type_and_format(
                &timestamp_to_string(ts),
                &Type::TIMESTAMPTZ,
                FieldFormat::Text,
            )?;
        } else {
            encoder.encode_field_with_type_and_format(
                &None::<String>,
                &Type::TIMESTAMPTZ,
                FieldFormat::Text,
            )?;
        }
    }

    // Encode tags in sorted order
    for key in &row_schema.tag_keys {
        let value = row.tags.iter().find(|t| &t.key == key).map(|t| &t.value);
        encoder.encode_field_with_type_and_format(&value, &Type::TEXT, FieldFormat::Text)?;
    }

    // Encode fields in sorted order
    for key in &row_schema.field_keys {
        if let Some(value) = row.fields.get(key) {
            let str_value = field_value_to_string(value);
            let pg_type = field_value_to_pg_type(value);
            encoder.encode_field_with_type_and_format(&str_value, &pg_type, FieldFormat::Text)?;
        } else {
            encoder.encode_field_with_type_and_format(
                &None::<String>,
                &Type::TEXT,
                FieldFormat::Text,
            )?;
        }
    }

    encoder.finish()
}

/// Convert a QueryResult to a PostgreSQL Response
pub fn query_result_to_response(result: QueryResult) -> PgWireResult<Response<'static>> {
    let schema = build_result_schema(&result);
    let row_schema = RowSchema::from_result(&result);
    let row_count = result.rows.len();

    // Encode all rows
    let mut data_rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        data_rows.push(encode_row(row, &schema, &row_schema)?);
    }

    // Create stream from encoded rows
    let stream = futures::stream::iter(data_rows.into_iter().map(Ok));

    let mut response = QueryResponse::new(schema, stream);
    response.set_command_tag(&format!("SELECT {}", row_count));

    Ok(Response::Query(response))
}

/// Build a simple schema for SHOW TABLES result (single TEXT column named "name")
pub fn build_tables_schema() -> Arc<Vec<FieldInfo>> {
    Arc::new(vec![FieldInfo::new(
        "name".to_string(),
        None,
        None,
        Type::TEXT,
        default_field_format(),
    )])
}

/// Encode a list of table names as a PostgreSQL response
pub fn tables_to_response(tables: Vec<String>) -> PgWireResult<Response<'static>> {
    let schema = build_tables_schema();
    let row_count = tables.len();

    let mut data_rows = Vec::with_capacity(tables.len());
    for name in tables {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field_with_type_and_format(&name, &Type::TEXT, FieldFormat::Text)?;
        data_rows.push(encoder.finish()?);
    }

    let stream = futures::stream::iter(data_rows.into_iter().map(Ok));

    let mut response = QueryResponse::new(schema, stream);
    response.set_command_tag(&format!("SELECT {}", row_count));

    Ok(Response::Query(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::{FieldValue, Tag};

    fn make_test_row(ts: i64, host: &str, value: f64) -> ResultRow {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Float(value));

        ResultRow {
            timestamp: Some(ts),
            series_id: 1,
            tags: vec![Tag::new("host", host)],
            fields,
        }
    }

    #[test]
    fn test_build_result_schema() {
        let result = QueryResult {
            measurement: "cpu".to_string(),
            rows: vec![
                make_test_row(1000, "server01", 1.0),
                make_test_row(2000, "server02", 2.0),
            ],
            total_rows: 2,
            execution_time_ns: 0,
        };

        let schema = build_result_schema(&result);
        assert_eq!(schema.len(), 3); // time, host, value

        assert_eq!(schema[0].name(), "time");
        assert_eq!(*schema[0].datatype(), Type::TIMESTAMPTZ);

        assert_eq!(schema[1].name(), "host");
        assert_eq!(*schema[1].datatype(), Type::TEXT);

        assert_eq!(schema[2].name(), "value");
        assert_eq!(*schema[2].datatype(), Type::FLOAT8);
    }

    #[test]
    fn test_row_schema() {
        let result = QueryResult {
            measurement: "cpu".to_string(),
            rows: vec![make_test_row(1000, "server01", 1.0)],
            total_rows: 1,
            execution_time_ns: 0,
        };

        let row_schema = RowSchema::from_result(&result);
        assert!(row_schema.has_time);
        assert_eq!(row_schema.tag_keys, vec!["host"]);
        assert_eq!(row_schema.field_keys, vec!["value"]);
    }

    #[test]
    fn test_build_tables_schema() {
        let schema = build_tables_schema();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name(), "name");
        assert_eq!(*schema[0].datatype(), Type::TEXT);
    }
}
