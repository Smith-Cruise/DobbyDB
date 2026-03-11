pub mod iceberg;
pub mod delta;
pub mod table_provider_factory;
pub mod hive;

#[derive(PartialEq)]
pub enum TableFormat {
    Iceberg,
    Delta,
    Hive
}