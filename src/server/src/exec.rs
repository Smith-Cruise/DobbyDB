use crate::DobbyDBServer;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::sync::Arc;

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(server: &DobbyDBServer) -> rustyline::Result<()> {
    let session_context = match server.create_session_context().await {
        Ok(session_context) => session_context,
        Err(err) => {
            eprintln!("{err}");
            return Ok(());
        }
    };
    let mut rl = DefaultEditor::new()?;
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
                rl.add_history_entry(trimmed)?;

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

                    // 这里可以添加实际执行 SQL 的逻辑
                    if let Err(e) = execute_sql(server, session_context.clone(), sql).await {
                        eprintln!("{e}");
                    }

                    // 清空缓冲区
                    sql_buffer.clear();
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("CTRL-C");
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
    Ok(())
}

async fn execute_sql(
    server: &DobbyDBServer,
    session_context: Arc<SessionContext>,
    sql: &str,
) -> Result<(), DataFusionError> {
    server.query(session_context, sql).await
}
