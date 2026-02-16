mod exec;

use clap::Parser;
use datafusion::common::error::Result;
use datafusion_cli::object_storage::instrumented::InstrumentedObjectStoreRegistry;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use dobbydb_catalog::catalog::get_catalog_manager;
use dobbydb_sql::session::ExtendedSessionContext;
use std::sync::Arc;

pub struct DobbyDBServer {
    config_path: String,
}

impl DobbyDBServer {
    fn new(config_path: String) -> Self {
        Self { config_path }
    }

    pub async fn init(&self) -> Result<()> {
        self.load_config()?;
        Ok(())
    }

    fn load_config(&self) -> Result<()> {
        let mut catalog_manager = get_catalog_manager().write().unwrap();
        catalog_manager.load_config(&self.config_path)?;
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct DobbyDBArgs {
    #[arg(short, long)]
    config: String,

    #[clap(
        long,
        num_args = 0..,
        help = "Execute the given command string(s), then exit. Commands are expected to be non empty.",
        value_parser(parse_command)
    )]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = DobbyDBArgs::parse();
    let server = DobbyDBServer::new(args.config.clone());
    server.init().await?;

    let print_options = PrintOptions {
        format: PrintFormat::Tsv,
        quiet: false,
        maxrows: MaxRows::Unlimited,
        color: true,
        instrumented_registry: Arc::new(InstrumentedObjectStoreRegistry::default()),
    };
    let session_context = ExtendedSessionContext::new().await?;
    let commands = args.command;
    if commands.is_empty() {
        exec::exec_from_repl(&session_context, &print_options).await;
    } else {
        exec::exec_from_commands(&session_context, commands, &print_options).await?;
    }
    Ok(())
}

fn parse_command(command: &str) -> Result<String, String> {
    if !command.is_empty() {
        Ok(command.to_string())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
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
