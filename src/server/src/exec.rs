use datafusion::common::Result;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::PrintOptions;
use dobbydb_sql::session::ExtendedSessionContext;
use futures::StreamExt;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Editor};
use std::time::Instant;
use tokio::signal;

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(ctx: &ExtendedSessionContext, print_options: &mut PrintOptions) {
    let mut rl: DefaultEditor = Editor::new().expect("created editor");
    // rl.set_helper(Some(CliHelper::new(
    //     &ctx.task_ctx().session_config().options().sql_parser.dialect,
    //     print_options.color,
    // )));
    rl.load_history(".history").ok();

    let print_options = print_options.clone();

    let mut sql_buffer = String::new();

    println!("DobbyDB SQL CLI - Enter your SQL commands (end with ;)");
    println!("Type 'quit;' to exit\n");

    loop {
        // 根据是否有未完成的语句选择提示符
        let prompt = if sql_buffer.is_empty() {
            "sql> "
        } else {
            "  -> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // 添加到历史记录
                rl.add_history_entry(trimmed).unwrap();

                // 将输入添加到缓冲区
                if !sql_buffer.is_empty() {
                    sql_buffer.push(' ');
                }
                sql_buffer.push_str(trimmed);

                // 检查是否以分号结束
                if sql_buffer.ends_with(';') {
                    // 移除末尾的分号
                    let sql = sql_buffer.trim_end_matches(';').trim();

                    // 检查是否是退出命令
                    if sql.eq_ignore_ascii_case("quit") {
                        println!("Goodbye!");
                        break;
                    }

                    tokio::select! {
                        // 这里可以添加实际执行 SQL 的逻辑
                        res = exec_and_print(&ctx, &print_options, sql) => match res {
                            Ok(_) => {}
                            Err(err) => eprintln!("{err}"),
                        },
                        _ = signal::ctrl_c() => {
                            // println!("^C");
                        },
                    }
                    // 清空缓冲区
                    sql_buffer.clear();
                }
            }
            Err(ReadlineError::Interrupted) => {
                sql_buffer.clear();
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

async fn exec_and_print(
    ctx: &ExtendedSessionContext,
    print_options: &PrintOptions,
    sql: &str,
) -> Result<()> {
    let now = Instant::now();
    let df = ctx.sql(sql).await?;
    let format_options = ctx.task_ctx().session_config().options().format.clone();

    if print_options.format == PrintFormat::Table {
        let mut stream = df.execute_stream().await?;
        let schema = stream.schema();
        let mut row_count = 0_usize;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            row_count += batch.num_rows();
            batches.push(batch);
        }
        print_options.print_batches(schema, &batches, now, row_count, &format_options)?;
    } else {
        let stream = df.execute_stream().await?;
        print_options
            .print_stream(stream, now, &format_options)
            .await?;
    }
    Ok(())
}
