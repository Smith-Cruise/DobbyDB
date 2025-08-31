use sqlparser::ast::Statement as SQLStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    SQLStatement(Box<SQLStatement>),
    ShowCatalogsStatement(ShowCatalogsStmt)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCatalogsStmt {

}