use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use crate::catalog::{get_catalog_manager, CatalogConfig};

pub const INTERNAL_CATALOG_NAME: &str = "internal";
pub const CATALOGS_TABLE_NAME: &str = "catalogs";

#[derive(Debug)]
pub struct InternalCatalog {
    information_schema: Arc<dyn SchemaProvider>
}

impl InternalCatalog {
    pub fn new() -> Self {
        Self {
            information_schema: Arc::new(MemorySchemaProvider::new())
        }
    }
}

impl CatalogProvider for InternalCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![String::from(INFORMATION_SCHEMA)]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == INFORMATION_SCHEMA {
            Some(self.information_schema.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct InformationSchemaShowCatalogs {
    schema: SchemaRef,
}

impl InformationSchemaShowCatalogs {
    pub fn new() -> Result<InformationSchemaShowCatalogs, DataFusionError> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
            Field::new("catalog_configs", DataType::Utf8, false),
        ]));

        let information_schema_catalogs = InformationSchemaShowCatalogs {
            schema: schema.clone(),
        };
        Ok(information_schema_catalogs)
    }
}

impl PartitionStream for InformationSchemaShowCatalogs {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {

        let mut catalog_name_builder = StringBuilder::new();
        let mut catalog_type_builder = StringBuilder::new();
        let mut catalog_configs_builder = StringBuilder::new();

        let catalog_manager = get_catalog_manager().read().unwrap();
        for (key, value) in catalog_manager.get_catalogs() {
            catalog_name_builder.append_value(key.clone());
            match value {
                CatalogConfig::HMS(hms_catalog) => {
                    catalog_type_builder.append_value("HMS");
                    catalog_configs_builder.append_value(format!("{:?}", hms_catalog));
                },
                CatalogConfig::GLUE(glue_catalog) => {
                    catalog_type_builder.append_value("GLUE");
                    catalog_configs_builder.append_value(format!("{:?}", glue_catalog));
                }
            }

        }
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(catalog_name_builder.finish()),
                Arc::new(catalog_type_builder.finish()),
                Arc::new(catalog_configs_builder.finish())
            ],
        ).unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}
