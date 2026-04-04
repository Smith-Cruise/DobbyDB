use sqlparser::ast::{ShowStatementFilter, Statement as SQLStatement};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCatalogsStatement {
    pub filter: Option<ShowStatementFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowVariablesStatement {
    pub filter: Option<ShowStatementFilter>,
    pub verbose: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtendedStatement {
    SQLStatement(Box<SQLStatement>),
    ShowCatalogsStatement(Box<ShowCatalogsStatement>),
    ShowVariablesStatement(Box<ShowVariablesStatement>),
}
