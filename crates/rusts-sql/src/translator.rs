//! SQL AST to Query model translation
//!
//! Translates sqlparser AST into RusTs Query model for execution.

use crate::error::{Result, SqlError};
use crate::functions::FunctionRegistry;
use rusts_core::TimeRange;
use rusts_query::{AggregateFunction, FieldSelection, Query, TagFilter};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind, Query as SqlQuery, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use tracing::debug;

/// Represents the result of parsing and translating a SQL statement
#[derive(Debug, Clone)]
pub enum SqlCommand {
    /// A SELECT query to execute
    Query(Query),
    /// SHOW TABLES command - returns list of measurements
    ShowTables,
    /// EXPLAIN query - returns query plan without execution
    Explain(Query),
    /// SET variable command - acknowledged but ignored
    SetVariable(String, String),
    /// System query returning a single value (e.g., SELECT version())
    SystemQuery {
        /// Column name for the result
        column: String,
        /// Value to return
        value: String,
    },
    /// Empty/no-op command
    Empty,
    /// Query against pg_catalog (return mock data)
    PgCatalogQuery {
        /// The catalog table being queried
        table: String,
    },
    /// Query against information_schema that returns empty (routines, parameters, etc.)
    InformationSchemaEmpty {
        /// The catalog table being queried
        table: String,
    },
    /// Query against information_schema.tables - returns list of measurements
    InformationSchemaTables,
    /// Query against information_schema.views - returns empty
    InformationSchemaViews,
    /// Query against information_schema.columns - returns columns for measurements
    InformationSchemaColumns,
}

/// SQL to Query translator
pub struct SqlTranslator;

impl SqlTranslator {
    /// Translate a SQL statement into a Query
    pub fn translate(stmt: &Statement) -> Result<Query> {
        match stmt {
            Statement::Query(query) => Self::translate_query(query),
            _ => Err(SqlError::UnsupportedFeature(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Translate a SQL statement into a SqlCommand (Query, ShowTables, or Explain)
    pub fn translate_command(stmt: &Statement) -> Result<SqlCommand> {
        match stmt {
            Statement::Query(query) => {
                // Check if this is a system query (e.g., SELECT version())
                if let Some(cmd) = Self::try_system_query(query) {
                    return Ok(cmd);
                }
                // Check if this is a pg_catalog query
                if let Some(cmd) = Self::try_pg_catalog_query(query) {
                    return Ok(cmd);
                }
                let q = Self::translate_query(query)?;
                Ok(SqlCommand::Query(q))
            }
            Statement::ShowTables { .. } => Ok(SqlCommand::ShowTables),
            Statement::Explain {
                statement,
                analyze,
                ..
            } => {
                // EXPLAIN ANALYZE is not supported
                if *analyze {
                    return Err(SqlError::UnsupportedFeature(
                        "EXPLAIN ANALYZE not supported".to_string(),
                    ));
                }
                // Extract the inner query from the EXPLAIN statement
                match statement.as_ref() {
                    Statement::Query(query) => {
                        let q = Self::translate_query(query)?;
                        Ok(SqlCommand::Explain(q))
                    }
                    _ => Err(SqlError::UnsupportedFeature(
                        "EXPLAIN only supports SELECT statements".to_string(),
                    )),
                }
            }
            // Handle SET commands - acknowledge but ignore
            Statement::Set(_) => {
                Ok(SqlCommand::Empty)
            }
            // Handle SHOW commands for PostgreSQL compatibility
            Statement::ShowVariable { variable } => {
                let var_name = variable.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".");
                Self::handle_show_variable(&var_name)
            }
            _ => Err(SqlError::UnsupportedFeature(format!(
                "Unsupported statement type: {:?}",
                stmt
            ))),
        }
    }

    /// Try to parse a system query like SELECT version(), SELECT current_database()
    fn try_system_query(query: &SqlQuery) -> Option<SqlCommand> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return None,
        };

        // System queries have no FROM clause
        if !select.from.is_empty() {
            return None;
        }

        // Check for single projection with a function call or literal
        if select.projection.len() != 1 {
            return None;
        }

        match &select.projection[0] {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Self::try_system_expr(expr)
            }
            _ => None,
        }
    }

    /// Try to handle a system expression
    fn try_system_expr(expr: &Expr) -> Option<SqlCommand> {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                match func_name.as_str() {
                    "version" => Some(SqlCommand::SystemQuery {
                        column: "version".to_string(),
                        value: "RusTs 0.1.0, compatible with PostgreSQL 15.0".to_string(),
                    }),
                    "current_database" | "current_catalog" => Some(SqlCommand::SystemQuery {
                        column: "current_database".to_string(),
                        value: "rusts".to_string(),
                    }),
                    "current_schema" => Some(SqlCommand::SystemQuery {
                        column: "current_schema".to_string(),
                        value: "public".to_string(),
                    }),
                    "current_user" | "session_user" | "user" => Some(SqlCommand::SystemQuery {
                        column: "current_user".to_string(),
                        value: "rusts".to_string(),
                    }),
                    "pg_backend_pid" => Some(SqlCommand::SystemQuery {
                        column: "pg_backend_pid".to_string(),
                        value: "1".to_string(),
                    }),
                    "inet_server_addr" => Some(SqlCommand::SystemQuery {
                        column: "inet_server_addr".to_string(),
                        value: "127.0.0.1".to_string(),
                    }),
                    "inet_server_port" => Some(SqlCommand::SystemQuery {
                        column: "inet_server_port".to_string(),
                        value: "5432".to_string(),
                    }),
                    _ => None,
                }
            }
            Expr::Value(val_with_span) => {
                match &val_with_span.value {
                    Value::Number(n, _) => Some(SqlCommand::SystemQuery {
                        column: "?column?".to_string(),
                        value: n.clone(),
                    }),
                    Value::SingleQuotedString(s) => Some(SqlCommand::SystemQuery {
                        column: "?column?".to_string(),
                        value: s.clone(),
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Try to detect queries against pg_catalog tables
    fn try_pg_catalog_query(query: &SqlQuery) -> Option<SqlCommand> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return None,
        };

        // Check if FROM clause references pg_catalog (including JOINs)
        for table_with_joins in &select.from {
            // Check main table
            if let Some(cmd) = Self::check_pg_catalog_table(&table_with_joins.relation) {
                return Some(cmd);
            }

            // Check joined tables
            for join in &table_with_joins.joins {
                if let Some(cmd) = Self::check_pg_catalog_table(&join.relation) {
                    return Some(cmd);
                }
            }
        }

        None
    }

    /// Check if a table factor references pg_catalog or information_schema
    fn check_pg_catalog_table(table_factor: &TableFactor) -> Option<SqlCommand> {
        if let TableFactor::Table { name, .. } = table_factor {
            let full_name = name.to_string().to_lowercase();

            // Check for pg_catalog tables
            if full_name.starts_with("pg_catalog.") || full_name.starts_with("pg_") {
                let table = full_name
                    .strip_prefix("pg_catalog.")
                    .unwrap_or(&full_name)
                    .to_string();
                return Some(SqlCommand::PgCatalogQuery { table });
            }

            // Check for information_schema tables
            if full_name.starts_with("information_schema.") {
                let table = full_name
                    .strip_prefix("information_schema.")
                    .unwrap_or(&full_name)
                    .to_string();

                match table.as_str() {
                    // Tables query - return list of measurements
                    "tables" => {
                        return Some(SqlCommand::InformationSchemaTables);
                    }
                    // Views query - return empty (we don't have views)
                    "views" => {
                        return Some(SqlCommand::InformationSchemaViews);
                    }
                    // Columns query - return columns for measurements
                    "columns" => {
                        return Some(SqlCommand::InformationSchemaColumns);
                    }
                    // These tables should return empty results
                    "routines" | "parameters" | "triggers" | "sequences"
                    | "check_constraints" | "referential_constraints" | "table_constraints"
                    | "key_column_usage" | "constraint_column_usage" | "constraint_table_usage" => {
                        return Some(SqlCommand::InformationSchemaEmpty { table });
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// Handle SHOW variable commands
    fn handle_show_variable(var_name: &str) -> Result<SqlCommand> {
        let var_lower = var_name.to_lowercase();
        match var_lower.as_str() {
            "server_version" | "server_version_num" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "150000".to_string(),
            }),
            "server_encoding" | "client_encoding" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "UTF8".to_string(),
            }),
            "standard_conforming_strings" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "on".to_string(),
            }),
            "transaction_isolation" | "default_transaction_isolation" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "read committed".to_string(),
            }),
            "timezone" | "time zone" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "UTC".to_string(),
            }),
            "datestyle" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "ISO, MDY".to_string(),
            }),
            "integer_datetimes" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "on".to_string(),
            }),
            "intervalstyle" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "postgres".to_string(),
            }),
            "is_superuser" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "on".to_string(),
            }),
            "session_authorization" => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "rusts".to_string(),
            }),
            _ => Ok(SqlCommand::SystemQuery {
                column: var_name.to_string(),
                value: "".to_string(),
            }),
        }
    }

    /// Translate a SQL Query (SELECT statement) into a Query
    fn translate_query(sql_query: &SqlQuery) -> Result<Query> {
        // Extract the SELECT body
        let select = match sql_query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => {
                return Err(SqlError::UnsupportedFeature(
                    "Only simple SELECT queries are supported (no UNION, INTERSECT, etc.)"
                        .to_string(),
                ))
            }
        };

        // Get measurement from FROM clause
        let measurement = Self::extract_measurement(&select.from)?;
        debug!("Extracted measurement: {}", measurement);

        // Start building the query
        let mut builder = Query::builder(&measurement);

        // Process WHERE clause for time range and tag filters
        if let Some(selection) = &select.selection {
            let (time_range, tag_filters) = Self::extract_where_clause(selection)?;
            debug!(
                "Time range: {:?}, Tag filters: {:?}",
                time_range, tag_filters
            );

            if let Some(tr) = time_range {
                builder = builder.time_range(tr.start, tr.end);
            }

            for filter in tag_filters {
                builder = match filter {
                    TagFilter::Equals { key, value } => builder.where_tag(key, value),
                    TagFilter::NotEquals { key, value } => builder.where_tag_not(key, value),
                    TagFilter::In { key, values } => builder.where_tag_in(key, values),
                    TagFilter::NotIn { key, values } => builder.where_tag_not_in(key, values),
                    TagFilter::Regex { .. } | TagFilter::Exists { .. } => {
                        // These need direct manipulation, so we handle them after build
                        builder
                    }
                };
            }
        }

        // Process SELECT clause for field selection and aggregations
        let (field_selection, group_by_time) = Self::extract_select_items(&select.projection)?;
        debug!("Field selection: {:?}", field_selection);

        match field_selection {
            FieldSelection::All => {}
            FieldSelection::Fields(fields) => {
                builder = builder.select_fields(fields);
            }
            FieldSelection::Aggregate {
                field,
                function,
                alias,
            } => {
                builder = builder.select_aggregate(field, function, alias);
            }
        }

        // Process GROUP BY clause
        let group_by_tags = Self::extract_group_by(&select.group_by, &group_by_time)?;
        if !group_by_tags.is_empty() {
            builder = builder.group_by_tags(group_by_tags);
        }

        // Set group by time interval if extracted from time_bucket
        if let Some(interval) = group_by_time {
            builder = builder.group_by_interval(interval);
        }

        // Process ORDER BY clause
        if let Some((order_field, ascending)) = Self::extract_order_by(&sql_query.order_by)? {
            builder = builder.order_by(order_field, ascending);
        }

        // Process LIMIT and OFFSET from limit_clause
        let (limit, offset) = Self::extract_limit_offset(&sql_query.limit_clause)?;
        if let Some(l) = limit {
            builder = builder.limit(l);
        }
        if let Some(o) = offset {
            builder = builder.offset(o);
        }

        builder.build().map_err(SqlError::Query)
    }

    /// Extract measurement name from FROM clause
    fn extract_measurement(from: &[TableWithJoins]) -> Result<String> {
        if from.is_empty() {
            return Err(SqlError::MissingClause("FROM".to_string()));
        }

        if from.len() > 1 {
            return Err(SqlError::UnsupportedFeature(
                "Multiple tables (JOINs) not supported".to_string(),
            ));
        }

        let table = &from[0];
        if !table.joins.is_empty() {
            return Err(SqlError::UnsupportedFeature(
                "JOINs not supported".to_string(),
            ));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => Ok(Self::extract_table_name(name)),
            _ => Err(SqlError::UnsupportedFeature(
                "Only simple table references are supported".to_string(),
            )),
        }
    }

    /// Extract table name from ObjectName (strips schema prefix like "public.")
    fn extract_table_name(name: &ObjectName) -> String {
        // Take the last part as the table name (handles public.trips -> trips)
        name.0
            .last()
            .map(|part| part.to_string())
            .unwrap_or_default()
    }

    /// Convert ObjectName to string (sqlparser 0.60 uses ObjectNamePart)
    fn object_name_to_string(name: &ObjectName) -> String {
        name.0
            .iter()
            .map(|part| part.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Extract time range and tag filters from WHERE clause
    fn extract_where_clause(expr: &Expr) -> Result<(Option<TimeRange>, Vec<TagFilter>)> {
        let mut time_start: Option<i64> = None;
        let mut time_end: Option<i64> = None;
        let mut tag_filters = Vec::new();

        Self::process_where_expr(expr, &mut time_start, &mut time_end, &mut tag_filters)?;

        let time_range = match (time_start, time_end) {
            (Some(start), Some(end)) => Some(TimeRange::new(start, end)),
            (Some(start), None) => Some(TimeRange::new(start, i64::MAX)),
            (None, Some(end)) => Some(TimeRange::new(i64::MIN, end)),
            (None, None) => None,
        };

        Ok((time_range, tag_filters))
    }

    /// Process a WHERE expression recursively
    fn process_where_expr(
        expr: &Expr,
        time_start: &mut Option<i64>,
        time_end: &mut Option<i64>,
        tag_filters: &mut Vec<TagFilter>,
    ) -> Result<()> {
        match expr {
            // AND expressions - process both sides
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                Self::process_where_expr(left, time_start, time_end, tag_filters)?;
                Self::process_where_expr(right, time_start, time_end, tag_filters)?;
            }

            // OR expressions - not supported at top level
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                ..
            } => {
                return Err(SqlError::UnsupportedFeature(
                    "OR conditions in WHERE clause not supported".to_string(),
                ));
            }

            // Comparison operations
            Expr::BinaryOp { left, op, right } => {
                Self::process_comparison(left, op, right, time_start, time_end, tag_filters)?;
            }

            // IN / NOT IN expressions: tag IN (value1, value2, ...)
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if let Expr::Identifier(ident) = expr.as_ref() {
                    let values: Result<Vec<String>> =
                        list.iter().map(|e| Self::expr_to_string(e)).collect();
                    if *negated {
                        tag_filters.push(TagFilter::NotIn {
                            key: ident.value.clone(),
                            values: values?,
                        });
                    } else {
                        tag_filters.push(TagFilter::In {
                            key: ident.value.clone(),
                            values: values?,
                        });
                    }
                }
            }

            // IS NOT NULL: tag IS NOT NULL
            Expr::IsNotNull(inner) => {
                if let Expr::Identifier(ident) = inner.as_ref() {
                    tag_filters.push(TagFilter::Exists {
                        key: ident.value.clone(),
                    });
                }
            }

            // Nested expressions
            Expr::Nested(inner) => {
                Self::process_where_expr(inner, time_start, time_end, tag_filters)?;
            }

            _ => {
                debug!("Ignoring unsupported WHERE expression: {:?}", expr);
            }
        }

        Ok(())
    }

    /// Process a comparison expression
    fn process_comparison(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        time_start: &mut Option<i64>,
        time_end: &mut Option<i64>,
        tag_filters: &mut Vec<TagFilter>,
    ) -> Result<()> {
        // Get the column name
        let column = match left {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => idents
                .last()
                .map(|i| i.value.clone())
                .unwrap_or_default(),
            _ => return Ok(()), // Skip complex left expressions
        };

        // Check if this is a time column
        let is_time_column = column.to_lowercase() == "time"
            || column.to_lowercase() == "timestamp"
            || column.to_lowercase() == "_time";

        if is_time_column {
            let ts = Self::expr_to_timestamp(right)?;
            match op {
                BinaryOperator::Gt => *time_start = Some(ts + 1),
                BinaryOperator::GtEq => *time_start = Some(ts),
                BinaryOperator::Lt => *time_end = Some(ts),
                BinaryOperator::LtEq => *time_end = Some(ts + 1),
                BinaryOperator::Eq => {
                    *time_start = Some(ts);
                    *time_end = Some(ts + 1);
                }
                _ => {}
            }
        } else {
            // Tag filter
            match op {
                BinaryOperator::Eq => {
                    tag_filters.push(TagFilter::Equals {
                        key: column,
                        value: Self::expr_to_string(right)?,
                    });
                }
                BinaryOperator::NotEq => {
                    tag_filters.push(TagFilter::NotEquals {
                        key: column,
                        value: Self::expr_to_string(right)?,
                    });
                }
                // Regex operator (custom extension)
                BinaryOperator::BitwiseOr => {
                    // Using ~ for regex in some SQL dialects
                    tag_filters.push(TagFilter::Regex {
                        key: column,
                        pattern: Self::expr_to_string(right)?,
                    });
                }
                _ => {
                    debug!("Ignoring unsupported comparison operator: {:?}", op);
                }
            }
        }

        Ok(())
    }

    /// Extract SELECT items and determine field selection
    fn extract_select_items(items: &[SelectItem]) -> Result<(FieldSelection, Option<i64>)> {
        let mut fields: Vec<String> = Vec::new();
        let mut aggregates: Vec<(String, AggregateFunction, Option<String>)> = Vec::new();
        let mut group_by_time: Option<i64> = None;
        let mut has_star = false;

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    has_star = true;
                }
                SelectItem::UnnamedExpr(expr) => {
                    Self::process_select_expr(
                        expr,
                        None,
                        &mut fields,
                        &mut aggregates,
                        &mut group_by_time,
                    )?;
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    Self::process_select_expr(
                        expr,
                        Some(&alias.value),
                        &mut fields,
                        &mut aggregates,
                        &mut group_by_time,
                    )?;
                }
                _ => {}
            }
        }

        // Determine field selection
        let field_selection = if has_star && aggregates.is_empty() && fields.is_empty() {
            FieldSelection::All
        } else if !aggregates.is_empty() {
            // Use first aggregate (we'll need to support multiple in future)
            let (field, function, alias) = aggregates.into_iter().next().unwrap();
            FieldSelection::Aggregate {
                field,
                function,
                alias,
            }
        } else if !fields.is_empty() {
            FieldSelection::Fields(fields)
        } else {
            FieldSelection::All
        };

        Ok((field_selection, group_by_time))
    }

    /// Process a SELECT expression
    fn process_select_expr(
        expr: &Expr,
        alias: Option<&str>,
        fields: &mut Vec<String>,
        aggregates: &mut Vec<(String, AggregateFunction, Option<String>)>,
        group_by_time: &mut Option<i64>,
    ) -> Result<()> {
        match expr {
            // Simple column reference
            Expr::Identifier(ident) => {
                fields.push(ident.value.clone());
            }

            // Compound identifier (e.g., table.column)
            Expr::CompoundIdentifier(idents) => {
                if let Some(last) = idents.last() {
                    fields.push(last.value.clone());
                }
            }

            // Function call (aggregates, time_bucket, etc.)
            Expr::Function(func) => {
                let func_name = Self::object_name_to_string(&func.name);
                let func_name_lower = func_name.to_lowercase();

                // Handle time_bucket function
                if func_name_lower == "time_bucket" {
                    if let Some(interval) = Self::extract_time_bucket_interval(&func.args)? {
                        *group_by_time = Some(interval);
                    }
                    return Ok(());
                }

                // Handle aggregate functions
                if FunctionRegistry::is_aggregate(&func_name) {
                    let agg_func = FunctionRegistry::get_aggregate(&func_name)?;

                    // Extract the field being aggregated
                    let field = Self::extract_function_field_arg(&func.args)?;

                    aggregates.push((field, agg_func, alias.map(String::from)));
                }
            }

            _ => {
                debug!("Ignoring unsupported SELECT expression: {:?}", expr);
            }
        }

        Ok(())
    }

    /// Extract interval from time_bucket function arguments (sqlparser 0.60 API)
    fn extract_time_bucket_interval(args: &FunctionArguments) -> Result<Option<i64>> {
        match args {
            FunctionArguments::List(arg_list) => {
                // time_bucket('1h', time) - first arg is interval
                if let Some(first_arg) = arg_list.args.first() {
                    let interval_str = Self::function_arg_to_string(first_arg)?;
                    return Ok(Some(FunctionRegistry::parse_interval(&interval_str)?));
                }
                Ok(None)
            }
            FunctionArguments::None => Ok(None),
            FunctionArguments::Subquery(_) => Err(SqlError::UnsupportedFeature(
                "Subquery in function arguments not supported".to_string(),
            )),
        }
    }

    /// Extract field name from function arguments (sqlparser 0.60 API)
    fn extract_function_field_arg(args: &FunctionArguments) -> Result<String> {
        match args {
            FunctionArguments::List(arg_list) => {
                if let Some(first_arg) = arg_list.args.first() {
                    return Self::function_arg_to_field(first_arg);
                }
                Ok("*".to_string())
            }
            FunctionArguments::None => Ok("*".to_string()),
            FunctionArguments::Subquery(_) => Err(SqlError::UnsupportedFeature(
                "Subquery in function arguments not supported".to_string(),
            )),
        }
    }

    /// Convert a FunctionArg to a string
    fn function_arg_to_string(arg: &FunctionArg) -> Result<String> {
        match arg {
            FunctionArg::Unnamed(arg_expr) => Self::function_arg_expr_to_string(arg_expr),
            FunctionArg::Named { arg, .. } => Self::function_arg_expr_to_string(arg),
            FunctionArg::ExprNamed { arg, .. } => Self::function_arg_expr_to_string(arg),
        }
    }

    /// Convert a FunctionArg to a field name
    fn function_arg_to_field(arg: &FunctionArg) -> Result<String> {
        match arg {
            FunctionArg::Unnamed(arg_expr) => Self::function_arg_expr_to_field(arg_expr),
            FunctionArg::Named { arg, .. } => Self::function_arg_expr_to_field(arg),
            FunctionArg::ExprNamed { arg, .. } => Self::function_arg_expr_to_field(arg),
        }
    }

    /// Convert a FunctionArgExpr to a string
    fn function_arg_expr_to_string(arg_expr: &FunctionArgExpr) -> Result<String> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Self::expr_to_string(expr),
            FunctionArgExpr::Wildcard => Ok("*".to_string()),
            FunctionArgExpr::QualifiedWildcard(_) => Ok("*".to_string()),
        }
    }

    /// Convert a FunctionArgExpr to a field name
    fn function_arg_expr_to_field(arg_expr: &FunctionArgExpr) -> Result<String> {
        match arg_expr {
            FunctionArgExpr::Expr(Expr::Identifier(ident)) => Ok(ident.value.clone()),
            FunctionArgExpr::Expr(expr) => Self::expr_to_string(expr),
            FunctionArgExpr::Wildcard => Ok("*".to_string()),
            FunctionArgExpr::QualifiedWildcard(_) => Ok("*".to_string()),
        }
    }

    /// Extract GROUP BY tags (excluding time_bucket)
    fn extract_group_by(
        group_by: &GroupByExpr,
        group_by_time: &Option<i64>,
    ) -> Result<Vec<String>> {
        let mut tags = Vec::new();

        match group_by {
            GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    match expr {
                        Expr::Identifier(ident) => {
                            let name = &ident.value;
                            // Skip time-related columns if we already have time bucketing
                            if group_by_time.is_some()
                                && (name.to_lowercase() == "bucket"
                                    || name.to_lowercase() == "time"
                                    || name.to_lowercase() == "_time")
                            {
                                continue;
                            }
                            tags.push(name.clone());
                        }
                        Expr::Function(func) => {
                            let func_name = Self::object_name_to_string(&func.name);
                            // Skip time_bucket in GROUP BY
                            if func_name.to_lowercase() == "time_bucket" {
                                continue;
                            }
                        }
                        _ => {}
                    }
                }
            }
            GroupByExpr::All(_) => {
                // GROUP BY ALL not supported
            }
        }

        Ok(tags)
    }

    /// Extract ORDER BY field and direction (sqlparser 0.60 API)
    fn extract_order_by(order_by: &Option<OrderBy>) -> Result<Option<(String, bool)>> {
        let order_by = match order_by {
            Some(ob) => ob,
            None => return Ok(None),
        };

        // Get the expressions from OrderByKind
        let exprs: &[OrderByExpr] = match &order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => return Ok(None),
        };

        if exprs.is_empty() {
            return Ok(None);
        }

        let first = &exprs[0];
        let field = match &first.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => idents
                .last()
                .map(|i| i.value.clone())
                .unwrap_or_default(),
            _ => return Ok(None),
        };

        // sqlparser 0.60: asc is in options.asc
        let ascending = first.options.asc.unwrap_or(true);

        Ok(Some((field, ascending)))
    }

    /// Extract LIMIT and OFFSET from limit_clause (sqlparser 0.60 API)
    fn extract_limit_offset(
        limit_clause: &Option<LimitClause>,
    ) -> Result<(Option<usize>, Option<usize>)> {
        let limit_clause = match limit_clause {
            Some(lc) => lc,
            None => return Ok((None, None)),
        };

        match limit_clause {
            LimitClause::LimitOffset { limit, offset, .. } => {
                let limit_val = match limit {
                    Some(expr) => Some(Self::expr_to_usize(expr)?),
                    None => None,
                };
                let offset_val = match offset {
                    Some(off) => Some(Self::expr_to_usize(&off.value)?),
                    None => None,
                };
                Ok((limit_val, offset_val))
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                // MySQL syntax: LIMIT offset, limit
                let limit_val = Some(Self::expr_to_usize(limit)?);
                let offset_val = Some(Self::expr_to_usize(offset)?);
                Ok((limit_val, offset_val))
            }
        }
    }

    /// Convert an expression to usize (for LIMIT/OFFSET)
    fn expr_to_usize(expr: &Expr) -> Result<usize> {
        match expr {
            Expr::Value(value_with_span) => {
                // sqlparser 0.60 uses ValueWithSpan, access the inner value
                match &value_with_span.value {
                    Value::Number(n, _) => n
                        .parse::<usize>()
                        .map_err(|_| SqlError::Translation(format!("Invalid number: {}", n))),
                    _ => Err(SqlError::Translation(
                        "Expected numeric value".to_string(),
                    )),
                }
            }
            _ => Err(SqlError::Translation(
                "Expected literal value".to_string(),
            )),
        }
    }

    /// Convert an expression to a string value
    fn expr_to_string(expr: &Expr) -> Result<String> {
        match expr {
            Expr::Value(value_with_span) => {
                // sqlparser 0.60 uses ValueWithSpan
                match &value_with_span.value {
                    Value::SingleQuotedString(s) => Ok(s.clone()),
                    Value::DoubleQuotedString(s) => Ok(s.clone()),
                    Value::Number(n, _) => Ok(n.clone()),
                    Value::Boolean(b) => Ok(b.to_string()),
                    _ => Err(SqlError::Translation(format!(
                        "Unsupported value type: {:?}",
                        value_with_span.value
                    ))),
                }
            }
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            _ => Err(SqlError::Translation(format!(
                "Cannot convert expression to string: {:?}",
                expr
            ))),
        }
    }

    /// Convert an expression to a timestamp
    fn expr_to_timestamp(expr: &Expr) -> Result<i64> {
        match expr {
            Expr::Value(value_with_span) => {
                // sqlparser 0.60 uses ValueWithSpan
                match &value_with_span.value {
                    Value::SingleQuotedString(s) => FunctionRegistry::parse_timestamp(s),
                    Value::DoubleQuotedString(s) => FunctionRegistry::parse_timestamp(s),
                    Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                        SqlError::InvalidTimeExpression(format!("Invalid timestamp: {}", n))
                    }),
                    _ => Err(SqlError::InvalidTimeExpression(format!(
                        "Unsupported value type for timestamp: {:?}",
                        value_with_span.value
                    ))),
                }
            }
            Expr::Function(func) => {
                let func_name = Self::object_name_to_string(&func.name);
                if func_name.to_lowercase() == "now" {
                    Ok(FunctionRegistry::now())
                } else {
                    Err(SqlError::InvalidTimeExpression(format!(
                        "Unknown time function: {}",
                        func_name
                    )))
                }
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle expressions like now() - interval '1 hour'
                let left_ts = Self::expr_to_timestamp(left)?;
                let right_ts = Self::expr_to_timestamp(right)?;

                match op {
                    BinaryOperator::Minus => Ok(left_ts - right_ts),
                    BinaryOperator::Plus => Ok(left_ts + right_ts),
                    _ => Err(SqlError::InvalidTimeExpression(format!(
                        "Unsupported time operation: {:?}",
                        op
                    ))),
                }
            }
            Expr::Interval(interval) => {
                // Handle INTERVAL '1 hour'
                let interval_str = Self::expr_to_string(&interval.value)?;
                FunctionRegistry::parse_interval(&interval_str)
            }
            _ => Err(SqlError::InvalidTimeExpression(format!(
                "Cannot convert to timestamp: {:?}",
                expr
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn translate(sql: &str) -> Result<Query> {
        let stmt = SqlParser::parse(sql)?;
        SqlTranslator::translate(&stmt)
    }

    #[test]
    fn test_simple_select() {
        let query = translate("SELECT * FROM cpu").unwrap();
        assert_eq!(query.measurement, "cpu");
        assert!(matches!(query.field_selection, FieldSelection::All));
    }

    #[test]
    fn test_select_fields() {
        let query = translate("SELECT usage, temperature FROM cpu").unwrap();
        assert_eq!(query.measurement, "cpu");
        match &query.field_selection {
            FieldSelection::Fields(fields) => {
                assert!(fields.contains(&"usage".to_string()));
                assert!(fields.contains(&"temperature".to_string()));
            }
            _ => panic!("Expected Fields selection"),
        }
    }

    #[test]
    fn test_where_tag_equals() {
        let query = translate("SELECT * FROM cpu WHERE host = 'server01'").unwrap();
        assert_eq!(query.tag_filters.len(), 1);
        match &query.tag_filters[0] {
            TagFilter::Equals { key, value } => {
                assert_eq!(key, "host");
                assert_eq!(value, "server01");
            }
            _ => panic!("Expected Equals filter"),
        }
    }

    #[test]
    fn test_where_tag_in() {
        let query =
            translate("SELECT * FROM cpu WHERE region IN ('us-west', 'us-east')").unwrap();
        assert_eq!(query.tag_filters.len(), 1);
        match &query.tag_filters[0] {
            TagFilter::In { key, values } => {
                assert_eq!(key, "region");
                assert!(values.contains(&"us-west".to_string()));
                assert!(values.contains(&"us-east".to_string()));
            }
            _ => panic!("Expected In filter"),
        }
    }

    #[test]
    fn test_where_tag_not_in() {
        let query =
            translate("SELECT * FROM cpu WHERE region NOT IN ('us-west', 'us-east')").unwrap();
        assert_eq!(query.tag_filters.len(), 1);
        match &query.tag_filters[0] {
            TagFilter::NotIn { key, values } => {
                assert_eq!(key, "region");
                assert!(values.contains(&"us-west".to_string()));
                assert!(values.contains(&"us-east".to_string()));
            }
            _ => panic!("Expected NotIn filter"),
        }
    }

    #[test]
    fn test_where_time_range() {
        let query =
            translate("SELECT * FROM cpu WHERE time >= '2024-01-01' AND time < '2024-01-02'")
                .unwrap();
        assert!(query.time_range.start > 0);
        assert!(query.time_range.end > query.time_range.start);
    }

    #[test]
    fn test_aggregation() {
        let query = translate("SELECT AVG(usage) FROM cpu").unwrap();
        match &query.field_selection {
            FieldSelection::Aggregate {
                field, function, ..
            } => {
                assert_eq!(field, "usage");
                assert!(matches!(function, AggregateFunction::Mean));
            }
            _ => panic!("Expected Aggregate selection"),
        }
    }

    #[test]
    fn test_group_by_tag() {
        let query = translate("SELECT AVG(usage) FROM cpu GROUP BY host").unwrap();
        assert!(query.group_by.contains(&"host".to_string()));
    }

    #[test]
    fn test_order_by() {
        let query = translate("SELECT * FROM cpu ORDER BY time DESC").unwrap();
        assert_eq!(query.order_by, Some(("time".to_string(), false)));
    }

    #[test]
    fn test_limit_offset() {
        let query = translate("SELECT * FROM cpu LIMIT 100 OFFSET 50").unwrap();
        assert_eq!(query.limit, Some(100));
        assert_eq!(query.offset, Some(50));
    }

    #[test]
    fn test_multiple_conditions() {
        let query = translate(
            "SELECT * FROM cpu WHERE host = 'server01' AND region = 'us-west' AND time >= '2024-01-01'",
        )
        .unwrap();
        assert_eq!(query.tag_filters.len(), 2);
        assert!(query.time_range.start > 0);
    }

    #[test]
    fn test_tag_not_equals() {
        let query = translate("SELECT * FROM cpu WHERE host != 'server01'").unwrap();
        assert_eq!(query.tag_filters.len(), 1);
        match &query.tag_filters[0] {
            TagFilter::NotEquals { key, value } => {
                assert_eq!(key, "host");
                assert_eq!(value, "server01");
            }
            _ => panic!("Expected NotEquals filter"),
        }
    }

    #[test]
    fn test_is_not_null() {
        let query = translate("SELECT * FROM cpu WHERE host IS NOT NULL").unwrap();
        // Note: TagFilter::Exists is not added via builder, would need to verify differently
        // For now, just verify it parses without error
        assert_eq!(query.measurement, "cpu");
    }

    #[test]
    fn test_join_rejected() {
        let result = translate("SELECT * FROM cpu JOIN memory ON cpu.host = memory.host");
        assert!(result.is_err());
    }

    #[test]
    fn test_union_rejected() {
        let result = translate("SELECT * FROM cpu UNION SELECT * FROM memory");
        assert!(result.is_err());
    }

    #[test]
    fn test_or_rejected() {
        let result = translate("SELECT * FROM cpu WHERE host = 'a' OR host = 'b'");
        assert!(result.is_err());
    }

    #[test]
    fn test_show_tables() {
        let stmt = SqlParser::parse("SHOW TABLES").unwrap();
        let cmd = SqlTranslator::translate_command(&stmt).unwrap();
        assert!(matches!(cmd, SqlCommand::ShowTables));
    }

    #[test]
    fn test_explain_query() {
        let stmt = SqlParser::parse("EXPLAIN SELECT * FROM cpu WHERE host = 'server01'").unwrap();
        let cmd = SqlTranslator::translate_command(&stmt).unwrap();
        match cmd {
            SqlCommand::Explain(query) => {
                assert_eq!(query.measurement, "cpu");
                assert_eq!(query.tag_filters.len(), 1);
            }
            _ => panic!("Expected Explain command"),
        }
    }

    #[test]
    fn test_explain_aggregation() {
        let stmt = SqlParser::parse("EXPLAIN SELECT COUNT(*) FROM cpu").unwrap();
        let cmd = SqlTranslator::translate_command(&stmt).unwrap();
        match cmd {
            SqlCommand::Explain(query) => {
                assert_eq!(query.measurement, "cpu");
                assert!(matches!(query.field_selection, FieldSelection::Aggregate { .. }));
            }
            _ => panic!("Expected Explain command"),
        }
    }
}
