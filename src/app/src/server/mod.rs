pub mod flight;
pub mod repl;

use crate::context::DobbyDbContext;
use crate::sql::session::ExtendedSessionContext;
use clap::Parser;
use datafusion::common::error::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_cli::object_storage::instrumented::{
    InstrumentedObjectStoreMode, InstrumentedObjectStoreRegistry,
};
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct DobbyDbArgs {
    #[clap(long, help = "Specify config path")]
    config: String,

    #[clap(
        long,
        help = "Specify the default object_store_profiling mode, defaults to 'disabled'.\n[possible values: disabled, summary, trace]",
        default_value_t = InstrumentedObjectStoreMode::Disabled
    )]
    object_store_profiling: InstrumentedObjectStoreMode,

    #[clap(long, help = "Specify the default catalog name")]
    default_catalog: Option<String>,

    #[clap(long, help = "Specify the default schema name")]
    default_schema: Option<String>,

    #[clap(
        long,
        num_args = 0..,
        help = "Execute the given command string(s), then exit. Commands are expected to be non empty.",
        value_parser(parse_command)
    )]
    command: Vec<String>,
}

pub fn run() -> Result<()> {
    let args = DobbyDbArgs::parse();
    let mut dobbydb_context = DobbyDbContext::new(Some(&args.config))?;
    dobbydb_context.default_catalog = args.default_catalog.clone();
    dobbydb_context.default_schema = args.default_schema.clone();
    let dobbydb_context = Arc::new(dobbydb_context);
    let cpu_handle = dobbydb_context.runtime_manager.cpu_handle();
    cpu_handle.block_on(async_run(dobbydb_context.clone(), args))
}

async fn async_run(dobbydb_context: Arc<DobbyDbContext>, args: DobbyDbArgs) -> Result<()> {
    let instrumented_registry = Arc::new(
        InstrumentedObjectStoreRegistry::new().with_profile_mode(args.object_store_profiling),
    );
    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_store_registry(instrumented_registry.clone())
        .build_arc()?;

    let print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Unlimited,
        color: true,
        instrumented_registry: instrumented_registry.clone(),
    };
    let session_context = ExtendedSessionContext::new(dobbydb_context, runtime_env);
    let commands = args.command;
    if commands.is_empty() {
        repl::exec_from_repl(&session_context, &print_options).await;
    } else {
        repl::exec_from_commands(&session_context, commands, &print_options).await?;
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
