use crate::table_format::TableFormat;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use std::collections::HashSet;

pub const ICEBERG_UNSUPPORTED_TABLE_DEFINITION: &str =
    "unsupported: SHOW CREATE TABLE is not supported for Iceberg tables";
pub const DELTA_UNSUPPORTED_TABLE_DEFINITION: &str =
    "unsupported: SHOW CREATE TABLE is not supported for Delta tables";

#[derive(Debug, Default)]
pub struct TableDefinitionBuilder {
    table_format: Option<TableFormat>,
    table_catalog: Option<String>,
    table_schema_name: Option<String>,
    table_name: Option<String>,
    table_schema: Option<TableSchema>,
    table_location: Option<String>,
}

impl TableDefinitionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_table_format(mut self, table_format: TableFormat) -> Self {
        self.table_format = Some(table_format);
        self
    }

    pub fn with_table_schema(
        mut self,
        table_catalog: impl Into<String>,
        table_schema_name: impl Into<String>,
        table_name: impl Into<String>,
        table_schema: TableSchema,
    ) -> Self {
        self.table_catalog = Some(table_catalog.into());
        self.table_schema_name = Some(table_schema_name.into());
        self.table_name = Some(table_name.into());
        self.table_schema = Some(table_schema);
        self
    }

    pub fn with_table_location(mut self, table_location: impl Into<String>) -> Self {
        self.table_location = Some(table_location.into());
        self
    }

    pub fn build(self) -> Result<String> {
        let table_format = self.required_table_format()?;
        match table_format {
            TableFormat::Hive => self.build_hive_definition(),
            TableFormat::Iceberg => Ok(ICEBERG_UNSUPPORTED_TABLE_DEFINITION.to_string()),
            TableFormat::Delta => Ok(DELTA_UNSUPPORTED_TABLE_DEFINITION.to_string()),
        }
    }

    fn build_hive_definition(self) -> Result<String> {
        let table_catalog = required(self.table_catalog, "table catalog")?;
        let table_schema_name = required(self.table_schema_name, "table schema")?;
        let table_name = required(self.table_name, "table name")?;
        let table_schema = required(self.table_schema, "table schema definition")?;
        let table_location = required(self.table_location, "table location")?;

        let mut definition = format!(
            "CREATE TABLE `{}`.`{}`.`{}`\n(\n",
            escape_backtick_identifier(&table_catalog),
            escape_backtick_identifier(&table_schema_name),
            escape_backtick_identifier(&table_name)
        );

        let partition_column_names = table_schema
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<HashSet<_>>();
        let columns = table_schema
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

        let partition_columns = table_schema.table_partition_cols();
        if !partition_columns.is_empty() {
            let partition_columns = partition_columns
                .iter()
                .map(|field| format!("`{}`", escape_backtick_identifier(field.name())))
                .collect::<Vec<_>>()
                .join(", ");
            definition.push_str(&format!("PARTITIONED BY ({partition_columns})\n"));
        }

        definition.push_str(&format!(
            "LOCATION '{}'",
            table_location.replace('\'', "''")
        ));
        Ok(definition)
    }

    fn required_table_format(&self) -> Result<TableFormat> {
        self.table_format
            .ok_or_else(|| DataFusionError::Internal("table format is required".to_string()))
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| DataFusionError::Internal(format!("{name} is required")))
}

fn escape_backtick_identifier(value: &str) -> String {
    value.replace('`', "``")
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

        let definition = TableDefinitionBuilder::new()
            .with_table_format(TableFormat::Hive)
            .with_table_schema("catalog", "schema", "table", schema)
            .with_table_location("s3://bucket/path")
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

        let definition = TableDefinitionBuilder::new()
            .with_table_format(TableFormat::Hive)
            .with_table_schema("catalog", "schema", "table", schema)
            .with_table_location("s3://bucket/path")
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

        let definition = TableDefinitionBuilder::new()
            .with_table_format(TableFormat::Hive)
            .with_table_schema("cat`alog", "schema", "ta`ble", schema)
            .with_table_location("s3://bucket/o'hara")
            .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` Utf8\n)\nUSING hive\nLOCATION 's3://bucket/o''hara'"
        );
        Ok(())
    }

    #[test]
    fn test_build_unsupported_definitions() -> Result<()> {
        assert_eq!(
            TableDefinitionBuilder::new()
                .with_table_format(TableFormat::Iceberg)
                .build()?,
            ICEBERG_UNSUPPORTED_TABLE_DEFINITION
        );
        assert_eq!(
            TableDefinitionBuilder::new()
                .with_table_format(TableFormat::Delta)
                .build()?,
            DELTA_UNSUPPORTED_TABLE_DEFINITION
        );
        Ok(())
    }
}
