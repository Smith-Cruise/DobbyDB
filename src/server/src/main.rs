use datafusion::arrow;
use datafusion::error::DataFusionError;
use dobbydb_catalog::catalog::get_catalog_manager;
use dobbydb_sql::parser::ExtendedParser;
use dobbydb_sql::planner::ExtendedQueryPlanner;
use futures::StreamExt;

pub struct DobbyDBServer {}

impl DobbyDBServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn init(&self) -> Result<(), DataFusionError> {
        self.load_config()?;
        Ok(())
    }

    fn load_config(&self) -> Result<(), DataFusionError> {
        let mut catalog_manager = get_catalog_manager().write().unwrap();
        catalog_manager.load_config("/Users/smith/Software/DobbyDbConfig/catalog.toml")?;
        Ok(())
    }

    pub async fn query(&self, query: &str) -> Result<(), DataFusionError> {
        let parser = ExtendedParser::parse_sql(query)?;
        if parser.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "Invalid query: {}",
                query
            )));
        }
        let stmt = &parser[0];
        let planner = ExtendedQueryPlanner::new()?;
        let logic_plan = planner.create_logical_plan(stmt).await?;
        let physical_plan = planner.create_physical_plan(&logic_plan).await?;
        let mut batch_stream = planner.execute_physical_plan(physical_plan).await?;
        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            println!("收到 batch，包含 {} 行", batch.num_rows());
            arrow::util::pretty::print_batches(&[batch])?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let server = DobbyDBServer::new();
    server.init().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server() -> Result<(), DataFusionError> {
        let server = DobbyDBServer::new();
        server.init().await?;
        server.query("show catalogs").await?;
        println!("{:?}", get_catalog_manager().read().unwrap());
        Ok(())
    }
}
