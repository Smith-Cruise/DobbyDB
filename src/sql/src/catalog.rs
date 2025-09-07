use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{
    CatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::any::Any;
use std::sync::Arc;

pub const INTERNAL_CATALOG_NAME: &str = "internal";
pub const DOBBYDB_SCHEMA_NAME: &str = "dobbydb";
pub const CATALOGS_TABLE_NAME: &str = "catalogs";

#[derive(Debug)]
pub struct InternalCatalog {
    dobbydb_schema: Arc<dyn SchemaProvider>
}

impl InternalCatalog {
    pub fn new() -> Self {
        Self {
            dobbydb_schema: Arc::new(MemorySchemaProvider::new())
        }
    }
}

impl CatalogProvider for InternalCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![String::from(DOBBYDB_SCHEMA_NAME)]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == DOBBYDB_SCHEMA_NAME {
            Some(self.dobbydb_schema.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct InformationSchemaCatalogs {
    schema: SchemaRef,
}

impl InformationSchemaCatalogs {
    pub fn new() -> Result<InformationSchemaCatalogs, DataFusionError> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
        ]));

        let information_schema_catalogs = InformationSchemaCatalogs {
            schema: schema.clone(),
        };
        Ok(information_schema_catalogs)
    }
}

impl PartitionStream for InformationSchemaCatalogs {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut catalog_name_builder = StringBuilder::new();
        let mut catalog_type_builder = StringBuilder::new();
        catalog_name_builder.append_value("hive_catalog");
        catalog_type_builder.append_value("HMS");
        catalog_name_builder.append_value("iceberg_catalog");
        catalog_type_builder.append_value("ICEBERG");
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(catalog_name_builder.finish()),
                Arc::new(catalog_type_builder.finish()),
            ],
        )
        .unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}
