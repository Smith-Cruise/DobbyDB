use datafusion::common::error::Result;

pub fn main() -> Result<()> {
    dobbydb_app::server::run()
}
