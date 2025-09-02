use crate::statements::ExtendedStatement;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    Expr, Extension, InvariantLevel, LogicalPlan,
    UserDefinedLogicalNodeCore,
};
use datafusion::prelude::SessionContext;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;

pub struct ExtendedQueryPlanner {
    session_context: SessionContext,
}

impl ExtendedQueryPlanner {
    pub fn new() -> Self {
        Self {
            session_context: SessionContext::new(),
        }
    }

    pub async fn create_logical_plan(
        &self,
        statement: ExtendedStatement,
    ) -> Result<LogicalPlan, DataFusionError> {
        match statement {
            ExtendedStatement::SQLStatement(stmt) => {
                let sql_string = stmt.to_string();
                let df = self.session_context.sql(&sql_string).await?;
                Ok(df.logical_plan().clone())
            }
            ExtendedStatement::ShowCatalogsStatement => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowCatalogsNode::new()?),
            })),
        }
    }
}

#[derive(Debug, Eq, Hash)]
pub struct ShowCatalogsNode {
    schema: DFSchemaRef,
}

impl ShowCatalogsNode {
    pub fn new() -> Result<Self, DataFusionError> {
        let arrow_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let df = DFSchema::try_from(arrow_schema)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self {
            schema: Arc::new(df),
        })
    }
}

impl PartialEq<Self> for ShowCatalogsNode {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.schema, &other.schema)
    }
}

impl PartialOrd for ShowCatalogsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ShowCatalogsNode {
    fn name(&self) -> &str {
        "ShowCatalogsNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(
        &self,
        check: InvariantLevel,
        plan: &LogicalPlan,
    ) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShowCatalogsNode")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(ShowCatalogsNode {
            schema: self.schema.clone(),
        })
    }
}
