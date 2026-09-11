use crate::catalog::TableDefinitionBuilder;
use crate::table_format::TableFormat;
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
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use deltalake::logstore::{
    LogStore, LogStoreFactory, LogStoreRef, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories,
};
use deltalake::{DeltaResult, DeltaTableBuilder};
use lakelet_storage::storage::{Storage, parse_location_schema_authority};
use std::sync::{Arc, OnceLock};
use url::Url;

/// Credential-free log-store factory. The object store passed to
/// `DeltaTableBuilder::with_storage_backend` carries per-catalog credentials;
/// this factory only satisfies delta-rs' requirement that every scheme has a
/// registered `LogStoreFactory` (the default registry covers memory/file only).
#[derive(Debug, Default)]
struct DefaultDeltaLogStoreFactory {}

impl LogStoreFactory for DefaultDeltaLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

fn register_delta_logstore_factories() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let factory = Arc::new(DefaultDeltaLogStoreFactory::default());
        for scheme in ["s3", "s3a", "oss", "hdfs"] {
            let url = Url::parse(&format!("{scheme}://")).unwrap();
            logstore_factories().insert(url, factory.clone());
        }
    });
}

#[derive(Debug)]
pub struct DeltaTableProvider {
    delta_scan: DeltaScanNext,
    log_store: LogStoreRef,
    table_definition: String,
}

impl DeltaTableProvider {
    pub async fn try_new(
        table_reference: TableReference,
        table_location: String,
        storage: Storage,
    ) -> Result<Self> {
        register_delta_logstore_factories();
        let (scheme, authority) = parse_location_schema_authority(&table_location)?;
        let store = storage.build_root_object_store(&scheme, &authority)?;
        let table_url =
            Url::parse(&table_location).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let builder = DeltaTableBuilder::from_url(table_url.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_storage_backend(store, table_url);
        let delta_table = builder
            .load()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let delta_scan = delta_table.table_provider().build().await?;
        let partition_column_names = delta_table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .metadata()
            .partition_columns()
            .to_vec();
        let table_definition = TableDefinitionBuilder::new(
            table_reference,
            table_location,
            TableFormat::Delta,
            delta_scan.schema().as_ref().clone(),
        )
        .with_partition_column_names(partition_column_names)
        .build()?;
        Ok(Self {
            delta_scan,
            log_store: delta_table.log_store(),
            table_definition,
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
    fn schema(&self) -> SchemaRef {
        self.delta_scan.schema()
    }

    fn table_type(&self) -> TableType {
        self.delta_scan.table_type()
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
