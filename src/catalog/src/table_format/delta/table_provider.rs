use crate::storage::StorageCredential;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use deltalake::delta_datafusion::DeltaScanNext;
use deltalake::logstore::{LogStore, LogStoreRef};
use deltalake::DeltaTableBuilder;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use url::Url;

#[derive(Debug)]
pub struct DeltaTableProvider {
    delta_scan: DeltaScanNext,
    log_store: LogStoreRef,
}

impl DeltaTableProvider {
    pub async fn try_new(
        _table_reference: &TableReference,
        table_location: &str,
        storage_credential: Option<StorageCredential>,
    ) -> Result<Self> {
        let storage_options = if let Some(storage_credential) = &storage_credential {
            storage_credential.build_delta_storage_options()
        } else {
            HashMap::new()
        };
        let table_url =
            Url::parse(table_location).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let builder = DeltaTableBuilder::from_url(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_allow_http(true)
            .with_storage_options(storage_options);
        let delta_table = builder
            .load()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let delta_scan = delta_table.table_provider().build().await?;
        Ok(Self {
            delta_scan,
            log_store: delta_table.log_store(),
        })
    }

    fn ensure_object_store_registered(&self, session: &dyn Session) -> Result<()> {
        let object_store_url = self.log_store.root_url().as_object_store_url();
        if session
            .runtime_env()
            .object_store(&object_store_url)
            .is_err()
        {
            session.runtime_env().register_object_store(
                object_store_url.as_ref(),
                self.log_store.root_object_store(None),
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.delta_scan.schema()
    }

    fn table_type(&self) -> TableType {
        self.delta_scan.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.ensure_object_store_registered(state)?;
        self.delta_scan
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.delta_scan.supports_filters_pushdown(filter)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.delta_scan.insert_into(state, input, insert_op).await
    }
}
