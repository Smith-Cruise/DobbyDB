#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableType {
    DataFiles,
    Partitions,
    Snapshots,
    Manifests,
}

impl TryFrom<&str> for MetadataTableType {
    type Error = String;

    fn try_from(metadata_table_name: &str) -> Result<Self, Self::Error> {
        match metadata_table_name {
            "data_files" => Ok(MetadataTableType::DataFiles),
            "partitions" => Ok(MetadataTableType::Partitions),
            "snapshots" => Ok(MetadataTableType::Snapshots),
            "manifests" => Ok(MetadataTableType::Manifests),
            _ => Err(format!(
                "invalid metadata table type: {metadata_table_name}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table_format::table_provider_factory::parse_table_reference;
    use crate::sql::parser::ExtendedParser;
    use crate::sql::statements::ExtendedStatement;
    use sqlparser::ast::{ObjectNamePart, SetExpr, Statement, TableFactor};

    /// Parse a `SELECT` and resolve its single table reference the same way the
    /// catalog providers do, so we exercise the full path from SQL text to
    /// `(table_name, MetadataTableType)`.
    fn resolve_table_reference(sql: &str) -> (String, Option<MetadataTableType>) {
        let statements = ExtendedParser::parse_sql(sql).expect("sql should parse");
        let ExtendedStatement::SQLStatement(stmt) = &statements[0] else {
            panic!("expected a SQL statement");
        };
        let Statement::Query(query) = stmt.as_ref() else {
            panic!("expected a query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a select");
        };
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            panic!("expected a table factor");
        };
        let table_ident = match name.0.last().expect("table name has a part") {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            other => panic!("unexpected object name part: {other:?}"),
        };
        parse_table_reference(&table_ident).expect("table reference should resolve")
    }

    #[test]
    fn parse_metadata_table() {
        // Unquoted `$` suffix: the case that used to fail before the dialect change.
        assert_eq!(
            resolve_table_reference("select * from orders$data_files"),
            ("orders".to_string(), Some(MetadataTableType::DataFiles))
        );

        // Backtick-quoted form resolves to the exact same result.
        assert_eq!(
            resolve_table_reference("select * from `orders$data_files`"),
            ("orders".to_string(), Some(MetadataTableType::DataFiles))
        );

        // Qualified name `db.table$type`, with and without quoting the table part.
        assert_eq!(
            resolve_table_reference("select * from sales.orders$snapshots"),
            ("orders".to_string(), Some(MetadataTableType::Snapshots))
        );
        assert_eq!(
            resolve_table_reference("select * from sales.`orders$snapshots`"),
            ("orders".to_string(), Some(MetadataTableType::Snapshots))
        );

        // A plain table without a `$` suffix carries no metadata type.
        assert_eq!(
            resolve_table_reference("select * from orders"),
            ("orders".to_string(), None)
        );
    }
}
