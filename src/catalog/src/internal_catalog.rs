use crate::catalog::{CatalogConfig, CatalogManager, DobbyDbCatalogProvider};
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{
    AsyncCatalogProvider, AsyncSchemaProvider, CatalogProvider, CatalogProviderList,
    SchemaProvider, TableProvider,
};
use datafusion::common::Result;
use datafusion::config::ConfigEntry;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::sync::Arc;
use crate::hms_catalog::HMSCatalog;

pub const INTERNAL_CATALOG: &str = "internal";
pub const INFORMATION_SCHEMA_SHOW_CATALOGS: &str = "catalogs";
pub const INFORMATION_SCHEMA_SHOW_SCHEMAS: &str = "schemas";
pub const INFORMATION_SCHEMA_SHOW_TABLES: &str = "tables";
pub const INFORMATION_SCHEMA_SHOW_VARIABLES: &str = "variables";

pub fn wrap_with_stream_table(table: Arc<dyn PartitionStream>) -> Result<Arc<StreamingTable>> {
    Ok(Arc::new(StreamingTable::try_new(
        table.schema().clone(),
        vec![table],
    )?))
}

#[derive(Debug)]
pub struct InternalCatalog {
    catalog_manager: Arc<CatalogManager>,
}

impl InternalCatalog {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self { catalog_manager }
    }
    // }
    // pub async fn try_new(catalog_provider_list: Arc<dyn CatalogProviderList>) -> Result<Self> {
    //     let information_schema = Arc::new(MemorySchemaProvider::new());
    //
    //     // show catalogs
    //     information_schema.register_table(
    //         INFORMATION_SCHEMA_SHOW_CATALOGS.to_string(),
    //         Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
    //             InformationSchemaShowCatalogs::new(),
    //         ))?),
    //     )?;
    //
    //     // show schemas
    //     information_schema.register_table(
    //         INFORMATION_SCHEMA_SHOW_SCHEMAS.to_string(),
    //         Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
    //             InformationSchemaShowSchemas::new(catalog_provider_list.clone()),
    //         ))?),
    //     )?;
    //
    //     // show tables
    //     information_schema.register_table(
    //         INFORMATION_SCHEMA_SHOW_TABLES.to_string(),
    //         Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
    //             InformationSchemaShowTables::new(catalog_provider_list.clone()),
    //         ))?),
    //     )?;
    //
    //     // show variables
    //     information_schema.register_table(
    //         INFORMATION_SCHEMA_SHOW_VARIABLES.to_string(),
    //         Arc::new(InternalCatalog::wrap_with_stream_table(Arc::new(
    //             InformationSchemaShowVariables::new(),
    //         ))?),
    //     )?;
    //     Ok(Self { information_schema })
    // }

    fn wrap_with_stream_table(table: Arc<dyn PartitionStream>) -> Result<StreamingTable> {
        StreamingTable::try_new(table.schema().clone(), vec![table])
    }
}

// impl CatalogProvider for InternalCatalog {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     fn schema_names(&self) -> Vec<String> {
//         vec![String::from(INFORMATION_SCHEMA)]
//     }
//
//     fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
//         if name == INFORMATION_SCHEMA {
//             Some(self.information_schema.clone())
//         } else {
//             None
//         }
//     }
// }

#[async_trait]
impl AsyncCatalogProvider for InternalCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        if schema_name == INFORMATION_SCHEMA {
            Ok(Some(Arc::new(InformationSchema::new(
                self.catalog_manager.clone(),
            ))))
        } else {
            Ok(None)
        }
    }
}

struct InformationSchema {
    catalog_manager: Arc<CatalogManager>,
}

impl InformationSchema {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self { catalog_manager }
    }
}

#[async_trait]
impl AsyncSchemaProvider for InformationSchema {
    async fn table(&self, table_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if table_name == INFORMATION_SCHEMA_SHOW_CATALOGS {
            Ok(Some(wrap_with_stream_table(Arc::new(
                InformationSchemaShowCatalogs::new(self.catalog_manager.clone()),
            ))?))
        } else if table_name == INFORMATION_SCHEMA_SHOW_SCHEMAS {
            Ok(Some(wrap_with_stream_table(Arc::new(
                InformationSchemaShowSchemas::new(self.catalog_manager.clone()),
            ))?))
        } else if table_name == INFORMATION_SCHEMA_SHOW_TABLES {
            todo!()
        } else if table_name == INFORMATION_SCHEMA_SHOW_VARIABLES {
            todo!()
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub struct InformationSchemaShowCatalogs {
    catalog_manager: Arc<CatalogManager>,
    schema: SchemaRef,
}

impl InformationSchemaShowCatalogs {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
            Field::new("catalog_config", DataType::Utf8, true),
        ]));

        InformationSchemaShowCatalogs {
            catalog_manager,
            schema,
        }
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

        let all_catalogs = self.catalog_manager.list_catalogs();
        for (catalog_name, catalog_config) in &all_catalogs {
            catalog_name_builder.append_value(catalog_name);
            if catalog_name == INTERNAL_CATALOG {
                catalog_type_builder.append_value("INTERNAL");
                catalog_configs_builder.append_null();
                continue;
            }

            match catalog_config {
                CatalogConfig::HMS(hms_catalog) => {
                    catalog_type_builder.append_value("HMS");
                    catalog_configs_builder.append_value(format!("{:?}", hms_catalog));
                }
                CatalogConfig::GLUE(glue_catalog) => {
                    catalog_type_builder.append_value("GLUE");
                    catalog_configs_builder.append_value(format!("{:?}", glue_catalog));
                }
                CatalogConfig::Internal => {
                    catalog_type_builder.append_value("INTERNAL");
                    catalog_configs_builder.append_null();
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
                return Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    futures::stream::once(async move { Err(error) }),
                ));
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
    catalog_manager: Arc<CatalogManager>,
    schema: SchemaRef,
}

impl InformationSchemaShowSchemas {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "schema_name",
            DataType::Utf8,
            false,
        )]));

        InformationSchemaShowSchemas {
            catalog_manager,
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
        let default_catalog = catalog.default_catalog.clone();
        let catalog_manager = self.catalog_manager.clone();
        let schema = Arc::clone(&self.schema);

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(async move {
                let catalog_config = if let Some(catalog_config) =
                    catalog_manager.get_catalog(&default_catalog)
                {
                    catalog_config
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Catalog '{}' not found",
                        default_catalog
                    )));
                };

                let all_schemas: Vec<String> = match catalog_config {
                    CatalogConfig::HMS(hms_catalog) => {
                        let hms_catalog = HMSCatalog::new(&Arc::new(hms_catalog.clone()));
                        hms_catalog.list_schema_names().await?
                    }
                    CatalogConfig::GLUE(_glue_catalog) => {
                        todo!()
                    }
                    CatalogConfig::Internal => {
                        todo!()
                    }
                };

                let mut schema_name_builder = StringBuilder::new();
                for schema_name in all_schemas {
                    schema_name_builder.append_value(schema_name);
                }

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(schema_name_builder.finish())],
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))
            }),
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

#[derive(Debug)]
struct InformationSchemaShowVariables {
    schema: SchemaRef,
}

impl InformationSchemaShowVariables {
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, false),
        ]));

        Self { schema }
    }

    fn append_setting(
        name_builder: &mut StringBuilder,
        value_builder: &mut StringBuilder,
        description_builder: &mut StringBuilder,
        entry: ConfigEntry,
    ) {
        name_builder.append_value(entry.key);
        value_builder.append_option(entry.value);
        description_builder.append_value(entry.description);
    }
}

impl PartitionStream for InformationSchemaShowVariables {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut name_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();
        let mut description_builder = StringBuilder::new();

        for entry in ctx.session_config().options().entries() {
            Self::append_setting(
                &mut name_builder,
                &mut value_builder,
                &mut description_builder,
                entry,
            );
        }

        for entry in ctx.runtime_env().config_entries() {
            Self::append_setting(
                &mut name_builder,
                &mut value_builder,
                &mut description_builder,
                entry,
            );
        }

        let batch = match RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(name_builder.finish()),
                Arc::new(value_builder.finish()),
                Arc::new(description_builder.finish()),
            ],
        ) {
            Ok(record_batch) => record_batch,
            Err(error) => {
                let error = DataFusionError::External(Box::new(error));
                return Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    futures::stream::once(async move { Err(error) }),
                ));
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move { Ok(batch) }),
        ))
    }
}
