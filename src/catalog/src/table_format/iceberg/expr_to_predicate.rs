use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::Datum;

// A datafusion expression could be an Iceberg predicate, column, or literal.
enum TransformedResult {
    Predicate(Predicate),
    Column(Reference),
    Literal(Datum),
    NotTransformed,
}

enum OpTransformedResult {
    Operator(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Converts DataFusion filters ([`Expr`]) to an iceberg [`Predicate`].
/// If none of the filters could be converted, return `None` which adds no predicates to the scan operation.
/// If the conversion was successful, return the converted predicates combined with an AND operator.
pub fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(convert_filter_to_predicate)
        .reduce(Predicate::and)
}

fn convert_filter_to_predicate(expr: &Expr) -> Option<Predicate> {
    match to_iceberg_predicate(expr) {
        TransformedResult::Predicate(predicate) => Some(predicate),
        TransformedResult::Column(_) | TransformedResult::Literal(_) => {
            unreachable!("Not a valid expression: {:?}", expr)
        }
        _ => None,
    }
}

fn to_iceberg_predicate(expr: &Expr) -> TransformedResult {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_iceberg_predicate(&binary.left);
            let right = to_iceberg_predicate(&binary.right);
            let op = to_iceberg_operation(binary.op);
            match op {
                OpTransformedResult::Operator(op) => to_iceberg_binary_predicate(left, right, op),
                OpTransformedResult::And => to_iceberg_and_predicate(left, right),
                OpTransformedResult::Or => to_iceberg_or_predicate(left, right),
                OpTransformedResult::NotTransformed => TransformedResult::NotTransformed,
            }
        }
        Expr::Not(exp) => {
            let expr = to_iceberg_predicate(exp);
            match expr {
                TransformedResult::Predicate(p) => TransformedResult::Predicate(!p),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Column(column) => TransformedResult::Column(Reference::new(column.name())),
        Expr::Literal(literal, _) => match scalar_value_to_datum(literal) {
            Some(data) => TransformedResult::Literal(data),
            None => TransformedResult::NotTransformed,
        },
        Expr::InList(inlist) => {
            let mut datums = vec![];
            for expr in &inlist.list {
                let p = to_iceberg_predicate(expr);
                match p {
                    TransformedResult::Literal(l) => datums.push(l),
                    _ => return TransformedResult::NotTransformed,
                }
            }

            let expr = to_iceberg_predicate(&inlist.expr);
            match expr {
                TransformedResult::Column(r) => match inlist.negated {
                    false => TransformedResult::Predicate(r.is_in(datums)),
                    true => TransformedResult::Predicate(r.is_not_in(datums)),
                },
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::IsNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNotNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::NotNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Cast(c) => {
            if c.data_type == DataType::Date32 || c.data_type == DataType::Date64 {
                // Casts to date truncate the expression, we cannot simply extract it as it
                // can create erroneous predicates.
                return TransformedResult::NotTransformed;
            }
            to_iceberg_predicate(&c.expr)
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_operation(op: Operator) -> OpTransformedResult {
    match op {
        Operator::Eq => OpTransformedResult::Operator(PredicateOperator::Eq),
        Operator::NotEq => OpTransformedResult::Operator(PredicateOperator::NotEq),
        Operator::Lt => OpTransformedResult::Operator(PredicateOperator::LessThan),
        Operator::LtEq => OpTransformedResult::Operator(PredicateOperator::LessThanOrEq),
        Operator::Gt => OpTransformedResult::Operator(PredicateOperator::GreaterThan),
        Operator::GtEq => OpTransformedResult::Operator(PredicateOperator::GreaterThanOrEq),
        // AND OR
        Operator::And => OpTransformedResult::And,
        Operator::Or => OpTransformedResult::Or,
        // Others not supported
        _ => OpTransformedResult::NotTransformed,
    }
}

fn to_iceberg_and_predicate(
    left: TransformedResult,
    right: TransformedResult,
) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.and(right))
        }
        (TransformedResult::Predicate(left), _) => TransformedResult::Predicate(left),
        (_, TransformedResult::Predicate(right)) => TransformedResult::Predicate(right),
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.or(right))
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_binary_predicate(
    left: TransformedResult,
    right: TransformedResult,
    op: PredicateOperator,
) -> TransformedResult {
    let (r, d, op) = match (left, right) {
        (TransformedResult::NotTransformed, _) => return TransformedResult::NotTransformed,
        (_, TransformedResult::NotTransformed) => return TransformedResult::NotTransformed,
        (TransformedResult::Column(r), TransformedResult::Literal(d)) => (r, d, op),
        (TransformedResult::Literal(d), TransformedResult::Column(r)) => {
            (r, d, reverse_predicate_operator(op))
        }
        _ => return TransformedResult::NotTransformed,
    };
    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(op, r, d)))
}

fn reverse_predicate_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        _ => unreachable!("Reverse {}", op),
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
/// Convert a scalar value to an iceberg datum.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
        _ => None,
    }
}