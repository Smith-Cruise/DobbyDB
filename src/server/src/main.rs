mod exec;
use datafusion::arrow;
use datafusion::catalog::{CatalogProviderList, MemoryCatalogProviderList};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use dobbydb_catalog::catalog::get_catalog_manager;
use dobbydb_catalog::internal_catalog::{
    InternalCatalog,
    INTERNAL_CATALOG,
};
use dobbydb_sql::parser::ExtendedParser;
use dobbydb_sql::planner::ExtendedQueryPlanner;
use futures::StreamExt;
use std::sync::Arc;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;

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

    pub async fn query(
        &self,
        session_context: Arc<SessionContext>,
        query: &str,
    ) -> Result<(), DataFusionError> {
        let parser = ExtendedParser::parse_sql(query)?;
        if parser.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "Invalid query: {}",
                query
            )));
        }

        let stmt = &parser[0];
        let planner = ExtendedQueryPlanner::new(session_context)?;
        let logic_plan = planner.create_logical_plan(stmt).await?;
        let physical_plan = planner.create_physical_plan(&logic_plan).await?;
        let mut batch_stream = planner.execute_physical_plan(physical_plan).await?;
        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            arrow::util::pretty::print_batches(&[batch])?;
        }
        Ok(())
    }

    pub async fn create_session_context(&self) -> Result<Arc<SessionContext>, DataFusionError> {
        let session_config = SessionConfig::new().with_default_catalog_and_schema(INTERNAL_CATALOG, INFORMATION_SCHEMA);
        let session_context = SessionContext::new_with_config(session_config);
        let memory_catalog_provider_list = Arc::new(MemoryCatalogProviderList::new());

        let catalog_manager = get_catalog_manager().read().unwrap();
        catalog_manager
            .register_into_catalog_list(memory_catalog_provider_list.clone())
            .await?;

        // load internal catalog
        memory_catalog_provider_list.register_catalog(
            INTERNAL_CATALOG.to_string(),
            Arc::new(InternalCatalog::try_new(memory_catalog_provider_list.clone()).await?),
        );

        session_context.register_catalog_list(memory_catalog_provider_list);
        Ok(Arc::new(session_context))
    }
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let server = DobbyDBServer::new();
    server.init().await?;
    exec::exec_from_repl(&server).await.map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server() -> Result<(), DataFusionError> {
        let server = DobbyDBServer::new();
        server.init().await?;
        let session_context = server.create_session_context().await?;
        server.query(session_context.clone(), "show catalogs").await?;
        server.query(session_context.clone(), "show schemas").await?;
        // println!("{:?}", get_catalog_manager().read().unwrap());
        Ok(())
    }
}
