use super::ExtendedSessionContext;
use datafusion::common::Result;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::sqlparser::ast::Use;

impl ExtendedSessionContext {
    pub(super) async fn handle_use_stmt(&self, use_stmt: &Use) -> Result<DataFrame> {
        let (catalog_name, schema_name) = match use_stmt {
            Use::Object(object_name) => {
                let object_name_vec = &object_name.0;
                match object_name_vec.len() {
                    1 => (
                        self.session_context
                            .state()
                            .config()
                            .options()
                            .catalog
                            .default_catalog
                            .clone(),
                        Some(object_name_vec[0].to_string()),
                    ),
                    2 => (
                        object_name_vec[0].to_string(),
                        Some(object_name_vec[1].to_string()),
                    ),
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "unsupported use stmt {:?}",
                            use_stmt
                        )));
                    }
                }
            }
            Use::Catalog(catalog_name) => {
                let object_name_vec = &catalog_name.0;
                (object_name_vec[0].to_string(), None)
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported use stmt {:?}",
                    use_stmt
                )));
            }
        };

        if !self
            .dobbydb_context
            .catalog_manager
            .catalog_exists(&catalog_name)
        {
            return Err(DataFusionError::Plan(format!(
                "unknown catalog {}",
                catalog_name
            )));
        }

        let state = self.session_context.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_catalog = catalog_name.clone();

        if let Some(schema_name) = schema_name {
            if !self
                .dobbydb_context
                .catalog_manager
                .schema_exist(&catalog_name, &schema_name)
                .await?
            {
                return Err(DataFusionError::Plan(format!(
                    "unknown schema {}",
                    schema_name
                )));
            }

            state
                .write()
                .config_mut()
                .options_mut()
                .catalog
                .default_schema = schema_name.clone();
        }

        let empty_logical_plan = LogicalPlanBuilder::empty(false).build()?;
        self.session_context
            .execute_logical_plan(empty_logical_plan)
            .await
    }
}
