pub mod delta;
pub mod hive;
pub mod iceberg;
mod metadata_table;
pub mod table_provider_factory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
    Delta,
    Hive,
}
