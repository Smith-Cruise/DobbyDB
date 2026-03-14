pub mod delta;
pub mod hive;
pub mod iceberg;
pub mod table_provider_factory;

#[derive(PartialEq)]
pub enum TableFormat {
    Iceberg,
    Delta,
    Hive,
}
