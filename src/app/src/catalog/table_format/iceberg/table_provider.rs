use crate::catalog::TableDefinitionBuilder;
use crate::table_format::TableFormat;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Transform;
use iceberg::table::Table;
use iceberg_datafusion::IcebergStaticTableProvider;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    inner: IcebergStaticTableProvider,
    table_definition: String,
}

impl IcebergTableProvider {
    pub async fn try_new_from_table(table_reference: TableReference, table: Table) -> Result<Self> {
        let table_definition = build_table_definition(&table_reference, &table)?;
        let inner = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(IcebergTableProvider {
            inner,
            table_definition,
        })
    }
}

fn build_table_definition(table_reference: &TableReference, table: &Table) -> Result<String> {
    let table_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_column_names = build_partition_column_names(table)?;

    TableDefinitionBuilder::new(
        TableFormat::Iceberg,
        table_reference.clone(),
        table_schema,
        table.metadata().location(),
    )
    .with_partition_column_names(partition_column_names)
    .build()
}

fn build_partition_column_names(table: &Table) -> Result<Vec<String>> {
    let iceberg_schema = table.metadata().current_schema();
    table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .filter(|partition_field| partition_field.transform != Transform::Void)
        .map(|partition_field| {
            let source_field = iceberg_schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "partition source field id {} not found",
                        partition_field.source_id
                    ))
                })?;
            Ok(source_field.name.clone())
        })
        .collect()
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.table_definition)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}
