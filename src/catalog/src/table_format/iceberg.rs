use crate::table_format::iceberg::metadata_table_provider::IcebergMetadataTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use std::collections::HashMap;
use std::sync::Arc;

mod metadata_scan;
pub mod metadata_table_provider;

pub struct IcebergTableProviderFactory {}

impl IcebergTableProviderFactory {
    pub async fn try_create_table_provider(
        metadata_location: &str,
        table_reference: &TableReference,
        metadata_table_name: Option<&str>,
        properties: HashMap<String, String>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
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
        let file_io = FileIO::from_path(metadata_location)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_props(properties)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::new(schema_name),
            name: table_name,
        };

        let iceberg_table =
            StaticTable::from_metadata_file(metadata_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_table();

        if let Some(metadata_table_name) = metadata_table_name {
            let metadata_table_provider =
                IcebergMetadataTableProvider::try_new(iceberg_table, metadata_table_name)?;
            Ok(Arc::new(metadata_table_provider))
        } else {
            let iceberg_table = IcebergTableProvider::try_new_from_table(iceberg_table)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Arc::new(iceberg_table))
        }
    }
}
