
pub fn get_publication_name(conn_name: &str) -> String {
    format!("{DB_PREFIX}{conn_name}")
}

pub async fn create_publication(
    client: &Client,
    pub_name: &str,
    table_identifiers: Option<&[TableIdentifier]>,
) -> Result<(), PostgresConnectorError> {
    let publication_name = get_publication_name(pub_name);
    let table_str: String = match table_identifiers {
        None => "ALL TABLES".to_string(),
        Some(table_identifiers) => {
            let table_names = table_identifiers
                .iter()
                .map(|table_identifier| {
                    format!(
                        r#""{}"."{}""#,
                        table_identifier
                            .schema
                            .as_deref()
                            .unwrap_or(DEFAULT_SCHEMA_NAME),
                        table_identifier.name
                    )
                })
                .collect::<Vec<_>>();
            format!("TABLE {}", table_names.join(" , "))
        }
    };

    client
        .simple_query(format!("DROP PUBLICATION IF EXISTS {publication_name}").as_str())
        .await
        .map_err(PostgresConnectorError::DropPublicationError)?;

    client
        .simple_query(format!("CREATE PUBLICATION {publication_name} FOR {table_str}").as_str())
        .await
        .map_err(PostgresConnectorError::CreatePublicationError)?;

    Ok(())
}
