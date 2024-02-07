use futures::{FutureExt, Stream, StreamExt};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, RelationBody, ReplicaIdentity, TupleData, UpdateBody, XLogDataBody,
};
use postgres_types::Type;
use std::collections::{hash_map::Entry, HashMap};
use thiserror::Error;
use tokio_postgres::Client;

use crate::threadpool::TokioRayonHandle;

use super::{
    field::Field,
    replication::{LogicalReplicationStream, ReplicationSlotParams},
    schema::{CdcType, PostgresTableInfo},
    types::{
        Operation, OperationMessage, PostgresConnectorError, PostgresSchemaError, Table,
        TableColumn,
    },
    util::postgres_type_to_field,
};

#[derive(Debug, Error)]
enum MappingError {
    #[error("Non-UTF8 column name")]
    NonUtf8ColumnName,
}


pub struct XlogStreamMapper {
    /// Relation id to table info from replication `Relation` message.
    relations_map: HashMap<u32, Table>,
    /// Relation id to (table index, column names).
    tables_columns: HashMap<u32, (usize, Vec<String>)>,
    /// Type id to type info from replication `Type` message.
    type_map: HashMap<u32, Type>,
}

impl XlogStreamMapper {
    pub async fn start(
        tables: Vec<PostgresTableInfo>,
        client: Client,
        options: ReplicationSlotParams,
    ) -> Result<impl Stream<Item = OperationMessage>, tokio_postgres::Error> {
        let tables_columns = tables
            .into_iter()
            .enumerate()
            .map(|(table_index, table_info)| {
                (table_info.relation_id, (table_index, table_info.columns))
            })
            .collect();

        let mapper = XlogStreamMapper {
            relations_map: HashMap::<u32, Table>::new(),
            tables_columns,
            type_map: HashMap::<u32, Type>::new(),
        };

        let stream = LogicalReplicationStream::start(client, options).await?;

        let mapped_stream = futures::stream::unfold((stream, mapper), |(mut stream, mapper)| {
            async move {
                let mut mapper = mapper;
                loop {
                    let Some(Ok(message)) = stream.next().await else {
                        return None;
                    };
                    // Is this a dumb hack? yes. Does it work? yes.
                    let (result, mapper2) =
                        TokioRayonHandle::spawn(move || mapper.handle_message_owned(&message)).await;
                    mapper = mapper2;

                    match result {
                        Ok(Some(val)) => return Some((val, (stream, mapper))),
                        Ok(None) | Err(_) => continue,
                    }
                }
            }
            .boxed()
        });

        Ok(mapped_stream)
    }
    pub fn handle_message_owned(
        mut self,
        message: &XLogDataBody<LogicalReplicationMessage>,
    ) -> (
        Result<Option<OperationMessage>, PostgresConnectorError>,
        Self,
    ) {
        (self.handle_message(message), self)
    }
    pub fn handle_message(
        &mut self,
        message: &XLogDataBody<LogicalReplicationMessage>,
    ) -> Result<Option<OperationMessage>, PostgresConnectorError> {
        match &message.data() {
            /*
            Every DML message contains a relation OID, identifying the publisher's relation that was acted on.
            Before the first DML message for a given relation OID, a Relation message will be sent, describing the schema of that relation.
            Subsequently, a new Relation message will be sent if the relation's definition has changed since the last Relation message was sent for it.
            (The protocol assumes that the client is capable of remembering this metadata for as many relations as needed.)
             */
            LogicalReplicationMessage::Relation(relation) => {
                self.ingest_schema(relation)?;
            }
            /*
            Relation messages identify column types by their OIDs. In the case of a built-in type,
            it is assumed that the client can look up that type OID locally, so no additional data is needed.
            For a non-built-in type OID, a Type message will be sent before the Relation message, to provide the type name associated with that OID.
            Thus, a client that needs to specifically identify the types of relation columns should cache the contents of Type messages,
            and first consult that cache to see if the type OID is defined there. If not, look up the type OID locally.
             */
            LogicalReplicationMessage::Type(msg) => {
                let oid = msg.id();
                let name = msg.name().unwrap_or("unknown_type").to_string();
                let schema = msg.namespace().unwrap_or("pg_catalog").to_string();
                let r#type = Type::new(name, oid, postgres_types::Kind::Simple, schema);
                self.type_map.insert(oid, r#type);
            }
            LogicalReplicationMessage::Insert(insert) => {
                let Some(table) = self.relations_map.get(&insert.rel_id()) else {
                    return Ok(None);
                };

                let values = insert.tuple().tuple_data();
                let fields = convert_values_to_fields(table, values, false)?;
                let new = reduce_message(&table.columns, &fields);

                return Ok(Some(OperationMessage {
                    table: table.name.clone(),
                    keys: table.keys.clone(),
                    operation: Operation::Insert { new },
                }));
            }
            LogicalReplicationMessage::Update(update) => {
                let Some(table) = self.relations_map.get(&update.rel_id()) else {
                    return Ok(None);
                };
                let values = update.new_tuple().tuple_data();

                let new_fields = convert_values_to_fields(table, values, false)?;
                let old_fields = convert_old_value_to_fields(table, update)?;

                let new = reduce_message(&table.columns, &new_fields);
                let old = reduce_message(&table.columns, &old_fields);

                return Ok(Some(OperationMessage {
                    table: table.name.clone(),
                    keys: table.keys.clone(),
                    operation: Operation::Update { old, new },
                }));
            }
            LogicalReplicationMessage::Delete(delete) => {
                let Some(table) = self.relations_map.get(&delete.rel_id()) else {
                    return Ok(None);
                };
                let values = delete
                    .key_tuple()
                    .or(delete.old_tuple())
                    .map(postgres_protocol::message::backend::Tuple::tuple_data)
                    .unwrap_or_default();

                let fields = convert_values_to_fields(table, values, true)?;
                let old = reduce_message(&table.columns, &fields);

                return Ok(Some(OperationMessage {
                    table: table.name.clone(),
                    keys: table.keys.clone(),
                    operation: Operation::Delete { old },
                }));
            }
            _ => {}
        }

        Ok(None)
    }

    fn ingest_schema(&mut self, relation: &RelationBody) -> Result<(), PostgresConnectorError> {
        let name = relation
            .name()
            .map_err(|_| PostgresSchemaError::NameError)?;

        let rel_id = relation.rel_id();
        let Some((table_index, wanted_columns)) = self.tables_columns.get(&rel_id) else {
            return Ok(());
        };

        let mut columns = vec![];
        for (column_index, column) in relation.columns().iter().enumerate() {
            let column_name =
                column
                    .name()
                    .map_err(|_| PostgresConnectorError::NonUtf8ColumnName {
                        table_index: *table_index,
                        column_index,
                    })?;

            if !wanted_columns.is_empty()
                && !wanted_columns
                    .iter()
                    .any(|column| column.as_str() == column_name)
            {
                continue;
            }

            let type_oid = column.type_id() as u32;

            let typ = self
                .get_type(type_oid)
                .ok_or_else(|| PostgresSchemaError::InvalidColumnType(column_name.to_string()))?;

            columns.push(TableColumn {
                name: column_name.to_string(),
                flags: column.flags(),
                r#type: typ,
                column_index,
            });
        }

        columns.sort_by_cached_key(|column| {
            wanted_columns
                .iter()
                .position(|wanted| wanted == &column.name)
                .expect("safe because we filtered on present keys above")
        });

        let replica_identity = match relation.replica_identity() {
            ReplicaIdentity::Default => ReplicaIdentity::Default,
            ReplicaIdentity::Nothing => ReplicaIdentity::Nothing,
            ReplicaIdentity::Full => ReplicaIdentity::Full,
            ReplicaIdentity::Index => ReplicaIdentity::Index,
        };

        let keys = columns
            .iter()
            .filter_map(|column| {
                if column.flags == 1 {
                    Some(column.name.clone())
                } else {
                    None
                }
            })
            .collect();

        let table = Table {
            name: name.to_string(),
            keys,
            columns,
            replica_identity: replica_identity.into(),
        };

        match self.relations_map.entry(rel_id) {
            Entry::Occupied(mut entry) => {
                // Check if type has changed.
                for (existing_column, column) in entry.get().columns.iter().zip(&table.columns) {
                    if existing_column.r#type != column.r#type {
                        return Err(PostgresConnectorError::ColumnTypeChanged {
                            table_index: *table_index,
                            column_name: existing_column.name.clone(),
                            old_type: existing_column.r#type.clone(),
                            new_type: column.r#type.clone(),
                        });
                    }
                }

                entry.insert(table);
            }
            Entry::Vacant(entry) => {
                entry.insert(table);
            }
        }
        Ok(())
    }

    fn get_type(&self, oid: u32) -> Option<Type> {
        Type::from_oid(oid).or_else(|| self.type_map.get(&oid).cloned())
    }
}

fn convert_values_to_fields(
    table: &Table,
    new_values: &[TupleData],
    only_key: bool,
) -> Result<Vec<Field>, PostgresConnectorError> {
    let mut values: Vec<Field> = vec![];

    for column in &table.columns {
        if column.flags == 1 || !only_key {
            let Some(value) = new_values.get(column.column_index) else {
                values.push(Field::Null);
                continue;
            };
            match value {
                TupleData::Null => values.push(
                    postgres_type_to_field(None, column)
                        .map_err(PostgresConnectorError::PostgresSchemaError)?,
                ),
                TupleData::UnchangedToast => {}
                TupleData::Text(text) => values.push(
                    postgres_type_to_field(Some(text), column)
                        .map_err(PostgresConnectorError::PostgresSchemaError)?,
                ),
            }
        } else {
            values.push(Field::Null);
        }
    }

    Ok(values)
}

fn convert_old_value_to_fields(
    table: &Table,
    update: &UpdateBody,
) -> Result<Vec<Field>, PostgresConnectorError> {
    match table.replica_identity {
        CdcType::OnlyPK | CdcType::FullChanges => update.key_tuple().map_or_else(
            || convert_values_to_fields(table, update.new_tuple().tuple_data(), true),
            |key_tuple| convert_values_to_fields(table, key_tuple.tuple_data(), true),
        ),
        CdcType::Nothing => Ok(vec![]),
    }
}

fn reduce_message(columns: &[TableColumn], fields: &[Field]) -> HashMap<String, Field> {
    let mut map = HashMap::new();
    for column in columns {
        let Some(field) = fields.get(column.column_index) else {
            continue;
        };

        map.insert(column.name.clone(), field.clone());
    }

    map
}
