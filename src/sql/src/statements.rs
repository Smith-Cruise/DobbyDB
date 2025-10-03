use sqlparser::ast::Statement as SQLStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtendedStatement {
    SQLStatement(Box<SQLStatement>),
    ShowCatalogsStatement,
}