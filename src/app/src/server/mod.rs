pub mod cli_helper;
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
use std::path::Path;
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
        help = "Execute the given command string, then exit. The command is expected to be non empty. Conflicts with --file.",
        value_parser(parse_command),
        conflicts_with = "file"
    )]
    command: Option<String>,

    #[clap(
        long,
        help = "Execute commands from a file, then exit. The file is expected to exist. Conflicts with --command.",
        value_parser(parse_file),
        conflicts_with = "command"
    )]
    file: Option<String>,
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
    let command = args.command;
    let file = args.file;
    if let Some(command) = command {
        repl::exec_from_commands(&session_context, &command, &print_options).await?;
    } else if let Some(file) = file {
        repl::exec_from_file(&session_context, &file, &print_options).await?;
    } else {
        repl::exec_from_repl(&session_context, &print_options).await;
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

fn parse_file(file: &str) -> Result<String, String> {
    if file.is_empty() {
        return Err("--file expects a non empty file path".to_string());
    }

    let path = Path::new(file);
    if path.is_file() {
        Ok(file.to_string())
    } else {
        Err(format!("--file expects an existing file path, got: {file}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_single_command() {
        let args = DobbyDbArgs::try_parse_from([
            "dobbydb",
            "--config",
            "config.toml",
            "--command",
            "show catalogs; show variables;",
        ])
        .expect("single command should parse");

        assert_eq!(
            args.command.as_deref(),
            Some("show catalogs; show variables;")
        );
    }

    #[test]
    fn test_parse_single_file() {
        let file = NamedTempFile::new().expect("temp sql file should be created");
        fs::write(file.path(), "show catalogs;show variables;")
            .expect("temp sql file should be written");
        let args = DobbyDbArgs::try_parse_from([
            "dobbydb",
            "--config",
            "config.toml",
            "--file",
            file.path()
                .to_str()
                .expect("temp sql file path should be valid utf-8"),
        ])
        .expect("single file should parse");

        let file_path = file
            .path()
            .to_str()
            .expect("temp sql file path should be valid utf-8");
        assert_eq!(args.file.as_deref(), Some(file_path));
    }
}
