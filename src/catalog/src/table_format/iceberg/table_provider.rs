use crate::table_format::iceberg::scan::IcebergTableScan;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::TableReference;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::table::{StaticTable, Table};
use iceberg::{NamespaceIdent, TableIdent};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct IcebergTableProvider {
    pub schema: SchemaRef,
    pub(crate) table: Table,
}

impl IcebergTableProvider {
    pub async fn try_new(
        table_reference: &TableReference,
        metadata_location: &str,
        properties: &HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        let schema_name: String;
        let table_name: String;
        match table_reference {
            TableReference::Full {
                catalog: _,
                schema,
                table,
            } => {
                schema_name = schema.to_string();
                table_name = table.to_string();
            }
            _ => {
                return Err(DataFusionError::Plan("invalid table reference".to_string()));
            }
        }
        let file_io = FileIO::from_path(metadata_location)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_props(properties)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::new(schema_name),
            name: table_name,
        };

        let iceberg_table =
            StaticTable::from_metadata_file(metadata_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_table();
        let schema = schema_to_arrow_schema(iceberg_table.metadata().current_schema())
            .map_err(|e| DataFusionError::External(e.into()))?;
        Ok(Self {
            schema: Arc::new(schema),
            table: iceberg_table,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let snapshot_id = self.table.metadata().current_snapshot_id();
        Ok(Arc::new(IcebergTableScan::new(
            self.table.clone(),
            snapshot_id,
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
