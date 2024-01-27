use std::collections::HashMap;

use super::{schema::{FieldDefinition, ListOrFilterColumns, PostgresTable, SchemaTableIdentifier, DEFAULT_SCHEMA_NAME}, types::PostgresSchemaError};

pub fn sort_schemas(
    expected_tables_order: &[ListOrFilterColumns],
    mapped_tables: &HashMap<SchemaTableIdentifier, PostgresTable>,
) -> Result<Vec<(SchemaTableIdentifier, PostgresTable)>, PostgresSchemaError> {
    let mut sorted_tables: Vec<(SchemaTableIdentifier, PostgresTable)> = Vec::new();
    for table in expected_tables_order.iter() {
        let table_identifier = (
            table
                .schema
                .clone()
                .unwrap_or(DEFAULT_SCHEMA_NAME.to_string()),
            table.name.clone(),
        );
        let postgres_table = mapped_tables
            .get(&table_identifier)
            .ok_or(PostgresSchemaError::ColumnNotFound)?;

        let sorted_table = table.columns.as_ref().map_or_else(
            || Ok::<PostgresTable, PostgresSchemaError>(postgres_table.clone()),
            |expected_order| {
                if expected_order.is_empty() {
                    Ok(postgres_table.clone())
                } else {
                    let sorted_fields = sort_fields(postgres_table, expected_order)?;
                    let mut new_table =
                        PostgresTable::new(postgres_table.replication_type().clone());
                    sorted_fields
                        .into_iter()
                        .for_each(|(f, is_index_field)| new_table.add_field(f, is_index_field));
                    Ok(new_table)
                }
            },
        )?;

        sorted_tables.push((table_identifier, sorted_table));
    }

    Ok(sorted_tables)
}

fn sort_fields(
    postgres_table: &PostgresTable,
    expected_order: &[String],
) -> Result<Vec<(FieldDefinition, bool)>, PostgresSchemaError> {
    let mut sorted_fields = Vec::new();

    for c in expected_order {
        let current_index = postgres_table
            .fields()
            .iter()
            .position(|f| c == &f.name)
            .ok_or(PostgresSchemaError::ColumnNotFound)?;

        let field = postgres_table
            .get_field(current_index)
            .ok_or(PostgresSchemaError::ColumnNotFound)?;
        let is_index_field = postgres_table
            .is_index_field(current_index)
            .ok_or(PostgresSchemaError::ColumnNotFound)?;

        sorted_fields.push((field.clone(), *is_index_field));
    }

    Ok(sorted_fields)
}
