use std::ascii::AsciiExt;
use datafusion::common::{Diagnostic, Span};
use datafusion::config::SqlParserOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::logical_expr::sqlparser::keywords::Keyword;
use datafusion::logical_expr::sqlparser::parser::{Parser, ParserError};
use datafusion::logical_expr::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
use std::collections::VecDeque;
use sqlparser::dialect::DatabricksDialect;
use crate::statements::{ExtendedStatement};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr $(; diagnostic = $DIAG:expr)?) => {{

        let err = DataFusionError::from(ParserError::ParserError($MSG.to_string()));
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }};
}

const DEFAULT_RECURSION_LIMIT: usize = 50;
const DEFAULT_DIALECT: DatabricksDialect = DatabricksDialect {};

pub struct ExtendedParserBuilder<'a> {
    /// The SQL string to parse
    sql: &'a str,
    /// The Dialect to use (defaults to [`GenericDialect`]
    dialect: &'a dyn Dialect,
    /// The recursion limit while parsing
    recursion_limit: usize,
}

impl<'a> ExtendedParserBuilder<'a> {
    /// Create a new parser builder for the specified tokens using the
    /// [`GenericDialect`].
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            dialect: &DEFAULT_DIALECT,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    pub fn build(self) -> Result<ExtendedParser<'a>, DataFusionError> {
        let mut tokenizer = Tokenizer::new(self.dialect, self.sql);
        // Convert TokenizerError -> ParserError
        let tokens = tokenizer
            .tokenize_with_location()
            .map_err(ParserError::from)?;

        Ok(ExtendedParser {
            parser: Parser::new(self.dialect)
                .with_tokens_with_locations(tokens)
                .with_recursion_limit(self.recursion_limit),
            options: SqlParserOptions {
                recursion_limit: self.recursion_limit,
                ..Default::default()
            },
        })
    }
}

pub struct ExtendedParser<'a> {
    parser: Parser<'a>,
    options: SqlParserOptions,
}

impl<'a> ExtendedParser<'a> {
    pub fn parse_sql(sql: &str) -> Result<VecDeque<ExtendedStatement>, DataFusionError> {
        let mut parser = ExtendedParserBuilder::new(sql).build()?;
        parser.parse_statements()
    }

    pub fn parse_statements(&mut self) -> Result<VecDeque<ExtendedStatement>, DataFusionError> {
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if self.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return self.expected("end of statement", self.parser.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    fn expected<T>(
        &self,
        expected: &str,
        found: TokenWithSpan,
    ) -> Result<T, DataFusionError> {
        let sql_parser_span = found.span;
        let span = Span::try_from_sqlparser_span(sql_parser_span);
        let diagnostic = Diagnostic::new_error(
            format!("Expected: {expected}, found: {found}{}", found.span.start),
            span,
        );
        parser_err!(
            format!("Expected: {expected}, found: {found}{}", found.span.start);
            diagnostic=
            diagnostic
        )
    }

    pub fn parse_statement(&mut self) -> Result<ExtendedStatement, DataFusionError> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::SHOW => {
                        self.parser.advance_token();
                        self.parse_show()
                    }
                    // Keyword::CREATE => {
                    //     self.parser.next_token(); // CREATE
                    //     self.parse_create()
                    // }
                    // Keyword::COPY => {
                    //     if let Token::Word(w) = self.parser.peek_nth_token(1).token {
                    //         // use native parser for COPY INTO
                    //         if w.keyword == Keyword::INTO {
                    //             return self.parse_and_handle_statement();
                    //         }
                    //     }
                    //     self.parser.next_token(); // COPY
                    //     self.parse_copy()
                    // }
                    // Keyword::EXPLAIN => {
                    //     self.parser.next_token(); // EXPLAIN
                    //     self.parse_explain()
                    // }
                    _ => {
                        // use sqlparser-rs parser
                        self.parse_and_handle_statement()
                    }
                }
            }
            _ => {
                // use the native parser
                self.parse_and_handle_statement()
            }
        }
    }

    fn parse_show(&mut self) -> Result<ExtendedStatement, DataFusionError> {
        if let token = self.parser.peek_token() {
            match &token.token {
                Token::Word(w) => {
                    let val = w.value.to_ascii_uppercase();
                    if val == "CATALOGS" {
                        self.parser.advance_token();
                        return Ok(ExtendedStatement::ShowCatalogsStatement);
                    }
                },
                _ => {}
            }
        }
        Ok(ExtendedStatement::SQLStatement(Box::from(self.parser.parse_show()?)))
    }

    /// Helper method to parse a statement and handle errors consistently, especially for recursion limits
    fn parse_and_handle_statement(&mut self) -> Result<ExtendedStatement, DataFusionError> {
        self.parser
            .parse_statement()
            .map(|stmt| ExtendedStatement::SQLStatement(Box::from(stmt)))
            .map_err(|e| match e {
                ParserError::RecursionLimitExceeded => DataFusionError::SQL(
                    Box::new(ParserError::RecursionLimitExceeded),
                    Some(format!(
                        " (current limit: {})",
                        self.options.recursion_limit
                    )),
                ),
                other => DataFusionError::SQL(Box::new(other), None),
            })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use sqlparser::ast::{ShowStatementOptions, Statement};
    use sqlparser::ast::Statement::ShowDatabases;
    use crate::planner::ExtendedQueryPlanner;
    use crate::statements::ExtendedStatement::SQLStatement;
    use super::*;

    #[test]
    fn test_show_catalogs() -> Result<(), DataFusionError> {
        let statement = ExtendedParser::parse_sql("show catalogs")?;
        let stmt = &statement[0];
        assert_eq!(ExtendedStatement::ShowCatalogsStatement, *stmt);
        Ok(())
    }

    #[test]
    fn test_show_databases() -> Result<(), DataFusionError> {
        let statement = ExtendedParser::parse_sql("show schemas")?;
        let stmt = &statement[0];
        let expected_statement = SQLStatement(Box::new(Statement::ShowSchemas {
            terse: false,
            history: false,
            show_options: ShowStatementOptions{
                show_in: None,
                starts_with: None,
                limit: None,
                limit_from: None,
                filter_position: None
            }
        }));
        assert_eq!(expected_statement, *stmt);
        Ok(())
    }

    #[test]
    fn test_simple_sql() -> Result<(), DataFusionError> {
        let statement = ExtendedParser::parse_sql("desc a")?;
        match &statement[0] {
            ExtendedStatement::SQLStatement(stmt) => {
                match stmt.as_ref() {
                    Statement::Use(use_stmt) => {
                        println!("use {:?}", use_stmt);
                    },
                    _ => {
                        println!("{:?}", stmt)
                    }
                }
            },
            _ => {
                println!("{:?}", statement[0]);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables_logical_plan() -> Result<(), DataFusionError> {
        // let statement = ExtendedParser::parse_sql("show tables")?;
        // println!("{:?}", statement);
        // let planner = ExtendedQueryPlanner::new()?;
        // let logical_plan = planner.create_logical_plan(&statement[0]).await?;
        // println!("{:?}", logical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan() -> Result<(), DataFusionError> {
        // let statement = ExtendedParser::parse_sql("show catalogs")?;
        // println!("{:?}", statement);
        // let planner = ExtendedQueryPlanner::new()?;
        // let logical_plan = planner.create_logical_plan(&statement[0]).await?;
        // println!("{:?}", logical_plan);
        // let physical_plan = planner.create_physical_plan(&logical_plan).await?;
        // println!("{:?}", physical_plan);
        // let mut batch_stream = planner.execute_physical_plan(physical_plan.clone()).await?;
        // while let Some(batch) = batch_stream.next().await {
        //     let batch = batch?;
        //     println!("收到 batch，包含 {} 行", batch.num_rows());
        //     arrow::util::pretty::print_batches(&[batch])?;
        // }
        // physical_plan.execute()
        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion() -> Result<(), DataFusionError> {
        let sql = "use test";
        let ctx = SessionContext::new();
        // ctx.register_table()
        let df = ctx.sql(sql).await?;
        let df = df.collect().await?;
        println!("{:?}", df);
        Ok(())
    }
}