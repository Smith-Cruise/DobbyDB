use crate::catalog::DobbyDbCatalogProvider;
use crate::context::DobbyDbContext;
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::Result;
use datafusion::config::ConfigEntry;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::sync::Arc;

pub const INTERNAL_CATALOG: &str = "internal";
pub const INFORMATION_SCHEMA_SHOW_VARIABLES: &str = "variables";

pub fn wrap_with_stream_table(table: Arc<dyn PartitionStream>) -> Result<Arc<StreamingTable>> {
    Ok(Arc::new(StreamingTable::try_new(
        table.schema().clone(),
        vec![table],
    )?))
}

pub struct InternalCatalog {
    _dobbydb_context: Arc<DobbyDbContext>,
}

impl InternalCatalog {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>) -> Self {
        Self {
            _dobbydb_context: dobbydb_context,
        }
    }
}

#[async_trait]
impl DobbyDbCatalogProvider for InternalCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        Ok(vec![INFORMATION_SCHEMA.to_string()])
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        if schema_name != INFORMATION_SCHEMA {
            return Err(DataFusionError::Plan(format!(
                "schema {} not exist",
                schema_name
            )));
        }
        Ok(vec![INFORMATION_SCHEMA_SHOW_VARIABLES.to_string()])
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        let schema_names = self.list_schema_names().await?;
        Ok(schema_names.iter().any(|name| name == schema_name))
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let table_names = self.list_table_names(schema_name).await?;
        Ok(table_names.iter().any(|name| name == table_name))
    }
}

#[async_trait]
impl AsyncCatalogProvider for InternalCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        if schema_name == INFORMATION_SCHEMA {
            Ok(Some(Arc::new(InformationSchema)))
        } else {
            Ok(None)
        }
    }
}

struct InformationSchema;

#[async_trait]
impl AsyncSchemaProvider for InformationSchema {
    async fn table(&self, table_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if table_name == INFORMATION_SCHEMA_SHOW_VARIABLES {
            Ok(Some(wrap_with_stream_table(Arc::new(
                InformationSchemaShowVariables::new(),
            ))?))
        } else {
            Ok(None)
        }
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
