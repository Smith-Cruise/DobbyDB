use crate::catalog::table_format::iceberg::iceberg_file_io::IcebergStorageFactory;
use crate::table_format::iceberg::iceberg_metadata_table_provider::IcebergMetadataTableProvider;
use crate::table_format::iceberg::iceberg_table_provider::IcebergTableProvider;
use crate::table_format::metadata_table::MetadataTableType;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use lakelet_storage::storage::{Storage, parse_location_schema_authority};
use std::sync::Arc;

pub(crate) mod iceberg_file_io;
mod iceberg_metadata_scan;
pub mod iceberg_metadata_table_provider;
pub(crate) mod iceberg_table_provider;

pub struct IcebergTableProviderFactory {}

impl IcebergTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: TableReference,
        table_location: String,
        metadata_location: String,
        metadata_table_type: Option<MetadataTableType>,
        storage: Storage,
    ) -> Result<Arc<dyn TableProvider>> {
        let (schema_name, table_name) = match &table_reference {
            TableReference::Full {
                catalog: _,
                schema,
                table,
            } => (schema.to_string(), table.to_string()),
            _ => {
                return Err(DataFusionError::Plan("invalid table reference".to_string()));
            }
        };
        let file_io = build_file_io(&metadata_location, &storage)?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::new(schema_name),
            name: table_name,
        };

        let iceberg_table =
            StaticTable::from_metadata_file(&metadata_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_table();

        if let Some(metadata_table_type) = metadata_table_type {
            let iceberg_metadata_table_provider =
                IcebergMetadataTableProvider::try_new(iceberg_table, metadata_table_type)?;
            Ok(Arc::new(iceberg_metadata_table_provider))
        } else {
            let iceberg_table = IcebergTableProvider::try_new_from_table(
                table_reference,
                table_location,
                iceberg_table,
            )
            .await?;
            Ok(Arc::new(iceberg_table))
        }
    }
}

fn build_file_io(metadata_location: &str, storage: &Storage) -> Result<FileIO> {
    // Validate scheme support and storage configuration up front so table
    // loading fails with a clear error instead of the first lazy file access.
    let (scheme, authority) = parse_location_schema_authority(metadata_location)?;
    if storage.build_operator(&scheme, &authority)?.is_none() {
        return Err(DataFusionError::Plan(format!(
            "no storage configured for scheme '{scheme}' of iceberg metadata location {metadata_location}"
        )));
    }

    Ok(FileIOBuilder::new(Arc::new(IcebergStorageFactory::new(storage.clone()))).build())
}
