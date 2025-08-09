use sqlparser::ast::Statement as SQLStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Statement(Box<SQLStatement>),
    ShowCatalogsStatement(ShowCatalogsStatement)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCatalogsStatement {

}