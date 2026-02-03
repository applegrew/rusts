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

/// Build the schema for pg_database table
pub fn build_pg_database_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datname".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("datdba".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("encoding".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datcollate".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("datctype".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("datistemplate".to_string(), None, None, Type::BOOL, default_field_format()),
        FieldInfo::new("datallowconn".to_string(), None, None, Type::BOOL, default_field_format()),
        FieldInfo::new("datconnlimit".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datlastsysoid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datfrozenxid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datminmxid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("dattablespace".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("datacl".to_string(), None, None, Type::TEXT, default_field_format()),
    ]
}

/// Build schema for pg_settings table
fn build_pg_settings_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("name".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("setting".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("unit".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("category".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("short_desc".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("extra_desc".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("context".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("vartype".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("source".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("min_val".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("max_val".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("enumvals".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("boot_val".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("reset_val".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("sourcefile".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("sourceline".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("pending_restart".to_string(), None, None, Type::BOOL, default_field_format()),
    ]
}

/// Build schema for pg_class table
fn build_pg_class_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("relname".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("relnamespace".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("reltype".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("reloftype".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("relowner".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("relkind".to_string(), None, None, Type::CHAR, default_field_format()),
    ]
}

/// Build schema for pg_type table
fn build_pg_type_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("typname".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("typnamespace".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("typowner".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("typlen".to_string(), None, None, Type::INT2, default_field_format()),
        FieldInfo::new("typbyval".to_string(), None, None, Type::BOOL, default_field_format()),
        FieldInfo::new("typtype".to_string(), None, None, Type::CHAR, default_field_format()),
        FieldInfo::new("typcategory".to_string(), None, None, Type::CHAR, default_field_format()),
        FieldInfo::new("typispreferred".to_string(), None, None, Type::BOOL, default_field_format()),
        FieldInfo::new("typisdefined".to_string(), None, None, Type::BOOL, default_field_format()),
        FieldInfo::new("typdelim".to_string(), None, None, Type::CHAR, default_field_format()),
        FieldInfo::new("typrelid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("typelem".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("typarray".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("relkind".to_string(), None, None, Type::CHAR, default_field_format()),
        FieldInfo::new("base_type_name".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("description".to_string(), None, None, Type::TEXT, default_field_format()),
    ]
}

/// Get schema for a pg_catalog table (used by describe in extended query protocol)
pub fn pg_catalog_schema(table: &str) -> Vec<FieldInfo> {
    match table {
        "pg_database" => build_pg_database_schema(),
        "pg_settings" => build_pg_settings_schema(),
        "pg_class" => build_pg_class_schema(),
        "pg_type" => build_pg_type_schema(),
        "pg_namespace" => build_pg_namespace_schema(),
        _ => Vec::new(),
    }
}

/// Build schema for pg_namespace table
fn build_pg_namespace_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("oid".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("nspname".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("nspowner".to_string(), None, None, Type::INT4, default_field_format()),
        FieldInfo::new("nspacl".to_string(), None, None, Type::TEXT, default_field_format()),
        FieldInfo::new("description".to_string(), None, None, Type::TEXT, default_field_format()),
    ]
}

/// Encode a pg_catalog response with real data where available
pub fn pg_catalog_response(table: &str, measurements: &[String]) -> PgWireResult<Response<'static>> {
    match table {
        "pg_database" => {
            // Return a single database "rusts"
            let schema = Arc::new(build_pg_database_schema());

            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field_with_type_and_format(&"16384".to_string(), &Type::INT4, FieldFormat::Text)?;  // oid
            encoder.encode_field_with_type_and_format(&"rusts".to_string(), &Type::TEXT, FieldFormat::Text)?;  // datname
            encoder.encode_field_with_type_and_format(&"10".to_string(), &Type::INT4, FieldFormat::Text)?;     // datdba
            encoder.encode_field_with_type_and_format(&"6".to_string(), &Type::INT4, FieldFormat::Text)?;      // encoding (UTF8)
            encoder.encode_field_with_type_and_format(&"en_US.UTF-8".to_string(), &Type::TEXT, FieldFormat::Text)?;  // datcollate
            encoder.encode_field_with_type_and_format(&"en_US.UTF-8".to_string(), &Type::TEXT, FieldFormat::Text)?;  // datctype
            encoder.encode_field_with_type_and_format(&"f".to_string(), &Type::BOOL, FieldFormat::Text)?;      // datistemplate
            encoder.encode_field_with_type_and_format(&"t".to_string(), &Type::BOOL, FieldFormat::Text)?;      // datallowconn
            encoder.encode_field_with_type_and_format(&"-1".to_string(), &Type::INT4, FieldFormat::Text)?;     // datconnlimit
            encoder.encode_field_with_type_and_format(&"12000".to_string(), &Type::INT4, FieldFormat::Text)?;  // datlastsysoid
            encoder.encode_field_with_type_and_format(&"722".to_string(), &Type::INT4, FieldFormat::Text)?;    // datfrozenxid
            encoder.encode_field_with_type_and_format(&"1".to_string(), &Type::INT4, FieldFormat::Text)?;      // datminmxid
            encoder.encode_field_with_type_and_format(&"1663".to_string(), &Type::INT4, FieldFormat::Text)?;   // dattablespace
            encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;       // datacl

            let data_row = encoder.finish()?;
            let stream = futures::stream::iter(vec![Ok(data_row)]);
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag("SELECT 1");
            Ok(Response::Query(response))
        }
        "pg_namespace" => {
            // Return public schema
            let schema = Arc::new(build_pg_namespace_schema());
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field_with_type_and_format(&"2200".to_string(), &Type::INT4, FieldFormat::Text)?;  // oid
            encoder.encode_field_with_type_and_format(&"public".to_string(), &Type::TEXT, FieldFormat::Text)?;  // nspname
            encoder.encode_field_with_type_and_format(&"10".to_string(), &Type::INT4, FieldFormat::Text)?;  // nspowner
            encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // nspacl
            encoder.encode_field_with_type_and_format(&"Standard public schema".to_string(), &Type::TEXT, FieldFormat::Text)?;  // description

            let data_row = encoder.finish()?;
            let stream = futures::stream::iter(vec![Ok(data_row)]);
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag("SELECT 1");
            Ok(Response::Query(response))
        }
        "pg_class" => {
            // Return measurements as tables
            let schema = Arc::new(build_pg_class_schema());
            let mut data_rows = Vec::with_capacity(measurements.len());

            for (i, measurement) in measurements.iter().enumerate() {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let oid = 20000 + i as i32;
                encoder.encode_field_with_type_and_format(&oid.to_string(), &Type::INT4, FieldFormat::Text)?;  // oid
                encoder.encode_field_with_type_and_format(measurement, &Type::TEXT, FieldFormat::Text)?;  // relname
                encoder.encode_field_with_type_and_format(&"2200".to_string(), &Type::INT4, FieldFormat::Text)?;  // relnamespace (public)
                encoder.encode_field_with_type_and_format(&"0".to_string(), &Type::INT4, FieldFormat::Text)?;  // reltype
                encoder.encode_field_with_type_and_format(&"0".to_string(), &Type::INT4, FieldFormat::Text)?;  // reloftype
                encoder.encode_field_with_type_and_format(&"10".to_string(), &Type::INT4, FieldFormat::Text)?;  // relowner
                encoder.encode_field_with_type_and_format(&"r".to_string(), &Type::CHAR, FieldFormat::Text)?;  // relkind (r = ordinary table)
                data_rows.push(encoder.finish()?);
            }

            let row_count = data_rows.len();
            let stream = futures::stream::iter(data_rows.into_iter().map(Ok));
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag(&format!("SELECT {}", row_count));
            Ok(Response::Query(response))
        }
        "pg_settings" => {
            // Return basic settings
            let schema = Arc::new(build_pg_settings_schema());
            let settings = vec![
                ("server_version", "15.0 (RusTs 0.1.0)", "", "Version", "Server version", "string", "default"),
                ("server_encoding", "UTF8", "", "Preset Options", "Server encoding", "string", "default"),
                ("client_encoding", "UTF8", "", "Client Connection Defaults", "Client encoding", "string", "default"),
                ("is_superuser", "on", "", "Preset Options", "Is superuser", "bool", "default"),
                ("TimeZone", "UTC", "", "Client Connection Defaults / Locale", "Time zone", "string", "default"),
            ];

            let mut data_rows = Vec::with_capacity(settings.len());
            for (name, setting, unit, category, desc, vartype, source) in settings {
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field_with_type_and_format(&name.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&setting.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&unit.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&category.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&desc.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // extra_desc
                encoder.encode_field_with_type_and_format(&"user".to_string(), &Type::TEXT, FieldFormat::Text)?;  // context
                encoder.encode_field_with_type_and_format(&vartype.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&source.to_string(), &Type::TEXT, FieldFormat::Text)?;
                encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // min_val
                encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // max_val
                encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // enumvals
                encoder.encode_field_with_type_and_format(&setting.to_string(), &Type::TEXT, FieldFormat::Text)?;  // boot_val
                encoder.encode_field_with_type_and_format(&setting.to_string(), &Type::TEXT, FieldFormat::Text)?;  // reset_val
                encoder.encode_field_with_type_and_format(&None::<String>, &Type::TEXT, FieldFormat::Text)?;  // sourcefile
                encoder.encode_field_with_type_and_format(&None::<i32>, &Type::INT4, FieldFormat::Text)?;  // sourceline
                encoder.encode_field_with_type_and_format(&"f".to_string(), &Type::BOOL, FieldFormat::Text)?;  // pending_restart
                data_rows.push(encoder.finish()?);
            }

            let row_count = data_rows.len();
            let stream = futures::stream::iter(data_rows.into_iter().map(Ok));
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag(&format!("SELECT {}", row_count));
            Ok(Response::Query(response))
        }
        "pg_type" => {
            // Return empty result with proper schema
            let schema = Arc::new(pg_catalog_schema(table));
            let stream = futures::stream::iter(vec![]);
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag("SELECT 0");
            Ok(Response::Query(response))
        }
        _ => {
            // Return empty result for other pg_catalog tables
            let schema = Arc::new(vec![]);
            let stream = futures::stream::iter(vec![]);
            let mut response = QueryResponse::new(schema, stream);
            response.set_command_tag("SELECT 0");
            Ok(Response::Query(response))
        }
    }
}

/// Encode a single value as a PostgreSQL response (for system queries like SELECT version())
pub fn single_value_response(column_name: &str, value: &str) -> PgWireResult<Response<'static>> {
    let schema = Arc::new(vec![FieldInfo::new(
        column_name.to_string(),
        None,
        None,
        Type::TEXT,
        default_field_format(),
    )]);

    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder.encode_field_with_type_and_format(&value.to_string(), &Type::TEXT, FieldFormat::Text)?;
    let data_row = encoder.finish()?;

    let stream = futures::stream::iter(vec![Ok(data_row)]);

    let mut response = QueryResponse::new(schema, stream);
    response.set_command_tag("SELECT 1");

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
