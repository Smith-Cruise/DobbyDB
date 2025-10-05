use async_trait::async_trait;
use datafusion::common::not_impl_err;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct ExtendedExecutor {}

#[async_trait]
impl QueryPlanner for ExtendedExecutor {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("xx")
    }
}
