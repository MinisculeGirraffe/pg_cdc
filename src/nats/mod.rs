use crate::postgres::types::{Operation, OperationMessage};

enum WireFormat {
    Json,
    MessagePack,
}
/// Key format
/// <table name>.<operation>.<key1>.<key2>.<key3>...
/// keys are sorted lexicographically from the column names
pub fn into_subject(op: &OperationMessage) -> String {
    let table = &op.table;
    op.keys.clone().sort();

    let op = match op.operation {
        Operation::Delete { .. } => "delete",
        Operation::Insert { .. } => "insert",
        Operation::Update { .. } => "update",
    };

    format!("{table}.{op}")
}
