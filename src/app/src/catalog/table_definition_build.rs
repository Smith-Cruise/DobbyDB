use crate::table_format::TableFormat;
use datafusion::common::TableReference;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use std::collections::HashSet;

#[derive(Debug)]
pub struct TableDefinitionBuilder {
    table_format: TableFormat,
    table_reference: TableReference,
    table_schema: TableSchema,
    location: String,
}

impl TableDefinitionBuilder {
    pub fn new(
        table_format: TableFormat,
        table_reference: TableReference,
        table_schema: TableSchema,
        location: impl Into<String>,
    ) -> Self {
        Self {
            table_format,
            table_reference,
            table_schema,
            location: location.into(),
        }
    }

    pub fn build(self) -> Result<String> {
        match self.table_format {
            TableFormat::Hive => self.build_hive_definition(),
            TableFormat::Iceberg | TableFormat::Delta => Err(DataFusionError::Internal(
                "table definition builder only supports Hive tables".to_string(),
            )),
        }
    }

    fn build_hive_definition(self) -> Result<String> {
        let (table_catalog, table_schema_name, table_name) =
            full_table_reference_parts(&self.table_reference)?;

        let mut definition = format!(
            "CREATE TABLE `{}`.`{}`.`{}`\n(\n",
            escape_backtick_identifier(table_catalog),
            escape_backtick_identifier(table_schema_name),
            escape_backtick_identifier(table_name)
        );

        let partition_column_names = self
            .table_schema
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<HashSet<_>>();
        let columns = self
            .table_schema
            .table_schema()
            .fields()
            .iter()
            .filter(|field| !partition_column_names.contains(field.name().as_str()))
            .map(|field| {
                format!(
                    "  `{}` {}",
                    escape_backtick_identifier(field.name()),
                    field.data_type()
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        definition.push_str(&columns);
        definition.push_str("\n)\nUSING hive\n");

        let partition_columns = self.table_schema.table_partition_cols();
        if !partition_columns.is_empty() {
            let partition_columns = partition_columns
                .iter()
                .map(|field| format!("`{}`", escape_backtick_identifier(field.name())))
                .collect::<Vec<_>>()
                .join(", ");
            definition.push_str(&format!("PARTITIONED BY ({partition_columns})\n"));
        }

        definition.push_str(&format!("LOCATION '{}'", self.location.replace('\'', "''")));
        Ok(definition)
    }
}

fn escape_backtick_identifier(value: &str) -> String {
    value.replace('`', "``")
}

fn full_table_reference_parts(table_reference: &TableReference) -> Result<(&str, &str, &str)> {
    match table_reference {
        TableReference::Full {
            catalog,
            schema,
            table,
        } => Ok((catalog.as_ref(), schema.as_ref(), table.as_ref())),
        _ => Err(DataFusionError::Internal(format!(
            "table reference must be fully qualified: {table_reference}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::table_schema::TableSchema;
    use std::sync::Arc;

    #[test]
    fn test_build_hive_definition_with_partitions() -> Result<()> {
        let schema = TableSchema::new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("amount", DataType::Decimal128(10, 2), true),
            ])),
            vec![Arc::new(Field::new("dt", DataType::Utf8, true))],
        );

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64,\n  `name` Utf8,\n  `amount` Decimal128(10, 2)\n)\nUSING hive\nPARTITIONED BY (`dt`)\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_definition_without_partitions() -> Result<()> {
        let schema = TableSchema::new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![],
        );

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64\n)\nUSING hive\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_definition_escapes_identifiers_and_location() -> Result<()> {
        let schema = TableSchema::new(
            Arc::new(Schema::new(vec![Field::new("a`b", DataType::Utf8, true)])),
            vec![],
        );

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("cat`alog", "schema", "ta`ble"),
            schema,
            "s3://bucket/o'hara",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` Utf8\n)\nUSING hive\nLOCATION 's3://bucket/o''hara'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_definition_requires_full_table_reference() {
        let schema = TableSchema::new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![],
        );

        let err = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::partial("schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()
        .unwrap_err();

        assert!(err.to_string().contains("fully qualified"));
    }
}
