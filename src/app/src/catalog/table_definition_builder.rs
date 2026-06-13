use crate::table_format::TableFormat;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::TableReference;
use datafusion::common::{DataFusionError, Result};

#[derive(Debug)]
pub struct TableDefinitionBuilder {
    table_format: TableFormat,
    table_reference: TableReference,
    table_schema: Schema,
    partition_column_names: Vec<String>,
    location: String,
}

impl TableDefinitionBuilder {
    pub fn new(
        table_format: TableFormat,
        table_reference: TableReference,
        table_schema: Schema,
        location: impl Into<String>,
    ) -> Self {
        Self {
            table_format,
            table_reference,
            table_schema,
            partition_column_names: vec![],
            location: location.into(),
        }
    }

    pub fn with_partition_column_names(mut self, partition_column_names: Vec<String>) -> Self {
        self.partition_column_names = partition_column_names;
        self
    }

    pub fn build(self) -> Result<String> {
        let (table_catalog, table_schema_name, table_name) =
            full_table_reference_parts(&self.table_reference)?;

        let mut definition = format!(
            "CREATE TABLE `{}`.`{}`.`{}`\n(\n",
            escape_backtick_identifier(table_catalog),
            escape_backtick_identifier(table_schema_name),
            escape_backtick_identifier(table_name)
        );

        let columns = self
            .table_schema
            .fields()
            .iter()
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
        definition.push_str("\n)\n");

        if !self.partition_column_names.is_empty() {
            let partition_columns = self
                .partition_column_names
                .iter()
                .map(|name| format!("`{}`", escape_backtick_identifier(name)))
                .collect::<Vec<_>>()
                .join(", ");
            definition.push_str(&format!("PARTITIONED BY ({partition_columns})\n"));
        }

        definition.push_str(&format!("USING {}\n", table_format_sql(self.table_format)));
        definition.push_str(&format!("LOCATION '{}'", escape_sql_string(&self.location)));
        Ok(definition)
    }
}

fn table_format_sql(table_format: TableFormat) -> &'static str {
    match table_format {
        TableFormat::Hive => "HIVE",
        TableFormat::Iceberg => "ICEBERG",
        TableFormat::Delta => "DELTA",
        TableFormat::Paimon => "PAIMON",
    }
}

fn escape_backtick_identifier(value: &str) -> String {
    value.replace('`', "``")
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
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

    #[test]
    fn test_build_hive_definition_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64,\n  `name` Utf8,\n  `amount` Decimal128(10, 2),\n  `dt` Utf8\n)\nPARTITIONED BY (`dt`)\nUSING HIVE\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_definition_without_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64\n)\nUSING HIVE\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_definition_escapes_identifiers_and_location() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a`b", DataType::Utf8, true)]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Hive,
            TableReference::full("cat`alog", "schema", "ta`ble"),
            schema,
            "s3://bucket/o'hara",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` Utf8\n)\nUSING HIVE\nLOCATION 's3://bucket/o''hara'"
        );
        Ok(())
    }

    #[test]
    fn test_build_iceberg_definition_without_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Iceberg,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64\n)\nUSING ICEBERG\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_iceberg_definition_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("dt", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Iceberg,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .with_partition_column_names(vec!["dt".to_string(), "id".to_string()])
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64,\n  `dt` Utf8,\n  `amount` Decimal128(10, 2)\n)\nPARTITIONED BY (`dt`, `id`)\nUSING ICEBERG\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_iceberg_definition_escapes_identifiers_and_location() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a`b", DataType::Utf8, true)]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Iceberg,
            TableReference::full("cat`alog", "schema", "ta`ble"),
            schema,
            "s3://bucket/o'hara",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `cat``alog`.`schema`.`ta``ble`\n(\n  `a``b` Utf8\n)\nUSING ICEBERG\nLOCATION 's3://bucket/o''hara'"
        );
        Ok(())
    }

    #[test]
    fn test_build_delta_definition_without_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Delta,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64\n)\nUSING DELTA\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_delta_definition_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Delta,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "s3://bucket/path",
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64,\n  `dt` Utf8\n)\nPARTITIONED BY (`dt`)\nUSING DELTA\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_paimon_definition_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dt", DataType::Utf8, true),
        ]);

        let definition = TableDefinitionBuilder::new(
            TableFormat::Paimon,
            TableReference::full("catalog", "schema", "table"),
            schema,
            "hdfs://namenode:8020/warehouse/schema.db/table",
        )
        .with_partition_column_names(vec!["dt".to_string()])
        .build()?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64,\n  `dt` Utf8\n)\nPARTITIONED BY (`dt`)\nUSING PAIMON\nLOCATION 'hdfs://namenode:8020/warehouse/schema.db/table'"
        );
        Ok(())
    }
}
