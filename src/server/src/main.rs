mod exec;

use clap::Parser;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::error::Result;
use datafusion::error::DataFusionError;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use dobbydb_catalog::catalog::get_catalog_manager;
use dobbydb_sql::session::ExtendedSessionContext;

pub struct DobbyDBServer {
    args: DobbyDBArgs,
}

impl DobbyDBServer {
    fn new(args: DobbyDBArgs) -> Self {
        Self { args }
    }

    pub async fn init(&self) -> Result<()> {
        self.load_config()?;
        Ok(())
    }

    fn load_config(&self) -> Result<()> {
        let mut catalog_manager = get_catalog_manager().write().unwrap();
        catalog_manager.load_config(&self.args.config)?;
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct DobbyDBArgs {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = DobbyDBArgs::parse();
    let server = DobbyDBServer::new(args);
    server.init().await?;

    let mut print_options = PrintOptions {
        format: PrintFormat::Tsv,
        quiet: false,
        maxrows: MaxRows::Unlimited,
        color: true,
    };
    let session_context = ExtendedSessionContext::new().await?;
    exec::exec_from_repl(&session_context, &mut print_options).await;
    Ok(())
}

pub async fn print_batches(batches: Vec<RecordBatch>) -> Result<(), DataFusionError> {
    arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server() -> Result<()> {
        // let args = DobbyDBArgs {
        //     config: "/Users/smith/Software/DobbyDbConfig/catalog.toml".to_string(),
        // };
        // let server = DobbyDBServer::new(args);
        // server.init().await?;
        // let session_context = ExtendedSessionContext::new().await?;
        //
        // // show catalogs
        // let df = session_context.sql("show catalogs").await?;
        // let batches = df.collect().await?;
        // print_batches(batches).await?;
        //
        // // show schemas
        // let df = session_context.sql("show schemas").await?;
        // let batches = df.collect().await?;
        // print_batches(batches).await?;
        Ok(())
    }
}
