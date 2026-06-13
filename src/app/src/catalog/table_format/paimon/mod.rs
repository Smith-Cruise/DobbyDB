mod paimon_table_provider;

use crate::table_format::paimon::paimon_table_provider::DobbyDbPaimonTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use dobbydb_storage::storage::{HDFS_SCHEMA, OSS_SCHEMA, S3_SCHEMA, S3A_SCHEMA, Storage};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::table::{SchemaManager, Table};
use paimon_datafusion::PaimonTableProvider;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub const PAIMON_INPUT_FORMAT: &str = "org.apache.paimon.hive.mapred.PaimonInputFormat";

pub struct PaimonTableProviderFactory;

impl PaimonTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: TableReference,
        table_location: String,
        storage: Option<Storage>,
    ) -> Result<Arc<dyn TableProvider>> {
        validate_storage_scheme(&table_location)?;

        let properties = storage
            .as_ref()
            .map(Storage::build_paimon_file_io_properties)
            .unwrap_or_default();
        let file_io = build_file_io(&table_location, properties)?;
        let schema = SchemaManager::new(file_io.clone(), table_location.clone())
            .latest()
            .await
            .map_err(to_datafusion_error)?
            .ok_or_else(|| {
                DataFusionError::External(
                    format!("Paimon schema not found at {table_location}").into(),
                )
            })?;
        let (database, table_name) = table_identifier_parts(&table_reference)?;
        let table = Table::new(
            file_io,
            Identifier::new(database, table_name),
            table_location.clone(),
            (*schema).clone(),
            None,
        );
        let inner = PaimonTableProvider::try_new(table)?;
        let provider = DobbyDbPaimonTableProvider::try_new(table_reference, table_location, inner)?;
        Ok(Arc::new(provider))
    }
}

fn build_file_io(table_location: &str, properties: HashMap<String, String>) -> Result<FileIO> {
    FileIO::from_path(table_location)
        .and_then(|builder| builder.with_props(properties).build())
        .map_err(to_datafusion_error)
}

fn validate_storage_scheme(table_location: &str) -> Result<()> {
    let url =
        Url::parse(table_location).map_err(|error| DataFusionError::External(Box::new(error)))?;
    match url.scheme() {
        S3_SCHEMA | S3A_SCHEMA | OSS_SCHEMA | HDFS_SCHEMA => Ok(()),
        scheme => Err(DataFusionError::NotImplemented(format!(
            "unsupported Paimon storage scheme: {scheme}"
        ))),
    }
}

fn table_identifier_parts(table_reference: &TableReference) -> Result<(String, String)> {
    match table_reference {
        TableReference::Full { schema, table, .. } => Ok((schema.to_string(), table.to_string())),
        _ => Err(DataFusionError::Plan(
            "Paimon table reference must be fully qualified".to_string(),
        )),
    }
}

fn to_datafusion_error(error: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
