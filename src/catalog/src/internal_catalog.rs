use crate::catalog::{get_catalog_manager, CatalogConfig};
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemorySchemaProvider,
    SchemaProvider,
};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::any::Any;
use std::sync::Arc;

pub const INTERNAL_CATALOG: &str = "internal";
pub const INFORMATION_SCHEMA_SHOW_CATALOGS: &str = "catalogs";
pub const INFORMATION_SCHEMA_SHOW_SCHEMAS: &str = "schemas";
pub const INFORMATION_SCHEMA_SHOW_TABLES: &str = "tables";

#[derive(Debug)]
pub struct InternalCatalog {
    information_schema: Arc<dyn SchemaProvider>,
}

impl InternalCatalog {
    pub async fn try_new(
        catalog_provider_list: Arc<dyn CatalogProviderList>,
    ) -> Result<Self, DataFusionError> {
        let information_schema = Arc::new(MemorySchemaProvider::new());

        // show catalogs
        information_schema.register_table(
            INFORMATION_SCHEMA_SHOW_CATALOGS.to_string(),
            Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
                InformationSchemaShowCatalogs::new(catalog_provider_list.clone()),
            ))?),
        )?;

        // show schemas
        information_schema.register_table(
            INFORMATION_SCHEMA_SHOW_SCHEMAS.to_string(),
            Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
                InformationSchemaShowSchemas::new(catalog_provider_list.clone()),
            ))?),
        )?;

        // show tables
        information_schema.register_table(
            INFORMATION_SCHEMA_SHOW_TABLES.to_string(),
            Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
                InformationSchemaShowTables::new(catalog_provider_list.clone()),
            ))?),
        )?;
        Ok(Self { information_schema })
    }

    fn wrap_with_stream_table(
        table: Arc<dyn PartitionStream>,
    ) -> Result<StreamingTable, DataFusionError> {
        Ok(StreamingTable::try_new(
            Arc::clone(&table.schema()),
            vec![table],
        )?)
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
    catalog_provider_list: Arc<dyn CatalogProviderList>,
    schema: SchemaRef,

}

impl InformationSchemaShowCatalogs {
    pub fn new(catalog_provider_list: Arc<dyn CatalogProviderList>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
            Field::new("catalog_config", DataType::Utf8, true),
        ]));

        InformationSchemaShowCatalogs { catalog_provider_list, schema }
    }
}

impl PartitionStream for InformationSchemaShowCatalogs {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut catalog_name_builder = StringBuilder::new();
        let mut catalog_type_builder = StringBuilder::new();
        let mut catalog_configs_builder = StringBuilder::new();

        let catalog_manager = get_catalog_manager().read().unwrap();
        let all_catalog_names = self.catalog_provider_list.catalog_names();
        for catalog_name in &all_catalog_names {
            catalog_name_builder.append_value(catalog_name);
            if catalog_name == INTERNAL_CATALOG {
                catalog_type_builder.append_value("INTERNAL");
                catalog_configs_builder.append_null();
                continue;
            }

            let catalog_config = catalog_manager.get_catalog(catalog_name);
            let catalog_config = match catalog_config {
                Some(config) => config,
                None => {
                    let error = DataFusionError::Internal(format!("catalog {} not found", catalog_name));
                    return Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&self.schema),
                        futures::stream::once(async move { Err(error) }),
                    ));
                },
            };

            match catalog_config {
                CatalogConfig::HMS(hms_catalog) => {
                    catalog_type_builder.append_value("HMS");
                    catalog_configs_builder.append_value(format!("{:?}", hms_catalog));
                }
                CatalogConfig::GLUE(glue_catalog) => {
                    catalog_type_builder.append_value("GLUE");
                    catalog_configs_builder.append_value(format!("{:?}", glue_catalog));
                }
            }
        }
        let batch = match RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(catalog_name_builder.finish()),
                Arc::new(catalog_type_builder.finish()),
                Arc::new(catalog_configs_builder.finish()),
            ],
        ) {
            Ok(record_batch) => record_batch,
            Err(error) => {
                let error = DataFusionError::External(Box::new(error));
                return Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), futures::stream::once(
                    async move { Err(error) },
                )));
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}

#[derive(Debug)]
pub struct InformationSchemaShowSchemas {
    catalog_provider_list: Arc<dyn CatalogProviderList>,
    schema: SchemaRef,
}

impl InformationSchemaShowSchemas {
    pub fn new(catalog_provider_list: Arc<dyn CatalogProviderList>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "schema_name",
            DataType::Utf8,
            false,
        )]));

        InformationSchemaShowSchemas {
            catalog_provider_list,
            schema,
        }
    }
}

impl PartitionStream for InformationSchemaShowSchemas {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog = &ctx.session_config().options().catalog;
        let default_catalog = &catalog.default_catalog;

        let catalog_provider = match self.catalog_provider_list.catalog(default_catalog) {
            Some(catalog_provider) => catalog_provider,
            None => {
                // catalog not found
                let error =
                    DataFusionError::Execution(format!("Catalog '{}' not found", default_catalog));
                return Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures::stream::once(async move { Err(error) }),
                ));
            }
        };

        let mut schema_name_builder = StringBuilder::new();
        for schema_name in &catalog_provider.schema_names() {
            schema_name_builder.append_value(schema_name.clone());
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(schema_name_builder.finish())],
        )
        .unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaShowTables {
    catalog_provider_list: Arc<dyn CatalogProviderList>,
    schema: SchemaRef,
}

impl InformationSchemaShowTables {
    pub fn new(catalog_provider_list: Arc<dyn CatalogProviderList>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "table_name",
            DataType::Utf8,
            false,
        )]));

        InformationSchemaShowTables {
            catalog_provider_list,
            schema,
        }
    }
}

impl PartitionStream for InformationSchemaShowTables {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog = &ctx.session_config().options().catalog;
        let default_catalog = &catalog.default_catalog;
        let default_schema = &catalog.default_schema;

        let catalog_provider = match self.catalog_provider_list.catalog(default_catalog) {
            Some(catalog_provider) => catalog_provider,
            None => {
                // catalog not found
                let error =
                    DataFusionError::Execution(format!("Catalog '{}' not found", default_catalog));
                return Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures::stream::once(async move { Err(error) }),
                ));
            }
        };

        let schema_provider = match catalog_provider.schema(default_schema) {
            Some(schema_provider) => schema_provider,
            None => {
                // schema not found
                let error = DataFusionError::Execution(format!(
                    "Schema '{}.{}' not found",
                    default_catalog, default_schema
                ));
                return Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures::stream::once(async move { Err(error) }),
                ));
            }
        };

        let mut table_name_builder = StringBuilder::new();
        for schema_name in &schema_provider.table_names() {
            table_name_builder.append_value(schema_name.clone());
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(table_name_builder.finish())],
        )
        .unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}
