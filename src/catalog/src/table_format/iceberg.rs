use crate::table_format::iceberg::metadata_table_provider::IcebergMetadataTableProvider;
use crate::table_format::iceberg::table_provider::IcebergTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use dobbydb_storage::storage::Storage;
use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory};
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

mod expr_to_predicate;
mod metadata_scan;
pub mod metadata_table_provider;
mod table_provider;
mod table_scan;

pub struct IcebergTableProviderFactory {}

impl IcebergTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: TableReference,
        metadata_location: String,
        metadata_table_name: Option<String>,
        storage: Option<Storage>,
    ) -> Result<Arc<dyn TableProvider>> {
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
        let file_io_properties = if let Some(storage) = &storage {
            storage.build_iceberg_file_io_properties()
        } else {
            HashMap::new()
        };
        let file_io = build_file_io(&metadata_location, file_io_properties)?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::new(schema_name),
            name: table_name,
        };

        let iceberg_table =
            StaticTable::from_metadata_file(&metadata_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_table();

        if let Some(metadata_table_name) = &metadata_table_name {
            let metadata_table_provider =
                IcebergMetadataTableProvider::try_new(iceberg_table, metadata_table_name)?;
            Ok(Arc::new(metadata_table_provider))
        } else {
            // using iceberg official sdk
            // let iceberg_table = IcebergTableProvider::try_new_from_table(iceberg_table)
            //     .await
            //     .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // Ok(Arc::new(iceberg_table))
            let iceberg_table =
                IcebergTableProvider::try_new_from_table(iceberg_table, storage).await?;
            Ok(Arc::new(iceberg_table))
        }
    }
}

fn build_file_io(
    metadata_location: &str,
    file_io_properties: HashMap<String, String>,
) -> Result<FileIO> {
    let parsed =
        Url::parse(metadata_location).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let scheme = parsed.scheme();
    let builder = match scheme {
        "file" => {
            return Ok(FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
                .with_props(file_io_properties)
                .build());
        }
        "s3" | "s3a" => FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: scheme.to_string(),
            customized_credential_load: None,
        })),
        "oss" => FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Oss)),
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported iceberg storage scheme: {scheme}"
            )));
        }
    };

    Ok(builder.with_props(file_io_properties).build())
}
