use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use std::collections::HashMap;

pub(crate) struct IcebergTableFormat {
    pub(crate) static_table: StaticTable,
}

impl IcebergTableFormat {
    pub async fn try_new(
        table_reference: &TableReference,
        metadata_location: &str,
        properties: &HashMap<String, String>,
    ) -> Result<IcebergTableFormat, DataFusionError> {
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
        let file_io_properties = properties.clone();
        let file_io = FileIO::from_path(metadata_location)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_props(file_io_properties)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::new(schema_name),
            name: table_name,
        };

        let iceberg_table =
            StaticTable::from_metadata_file(metadata_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            static_table: iceberg_table,
        })
    }
}
