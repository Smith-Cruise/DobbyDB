use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::DataFusionError;
use datafusion::common::Result;

pub fn hive_type_to_arrow_type(hive_type: &str) -> Result<DataType> {
    let lower = hive_type.trim().to_lowercase();
    let lower = lower.as_str();

    if lower == "int" || lower == "integer" {
        return Ok(DataType::Int32);
    }
    if lower == "bigint" || lower == "long" {
        return Ok(DataType::Int64);
    }
    if lower == "smallint" {
        return Ok(DataType::Int16);
    }
    if lower == "tinyint" {
        return Ok(DataType::Int8);
    }
    if lower == "float" {
        return Ok(DataType::Float32);
    }
    if lower == "double" || lower == "double precision" {
        return Ok(DataType::Float64);
    }
    if lower == "boolean" {
        return Ok(DataType::Boolean);
    }
    if lower == "string" || lower == "binary string" {
        return Ok(DataType::Utf8);
    }
    if lower.starts_with("varchar") || lower.starts_with("char") {
        return Ok(DataType::Utf8);
    }
    if lower == "binary" {
        return Ok(DataType::Binary);
    }
    if lower == "date" {
        return Ok(DataType::Date32);
    }
    if lower == "timestamp" {
        return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
    }
    if lower.starts_with("decimal") {
        return parse_decimal_type(hive_type);
    }

    Err(DataFusionError::NotImplemented(format!(
        "unsupported Hive column type: {}",
        hive_type
    )))
}

fn parse_decimal_type(hive_type: &str) -> Result<DataType> {
    let s = hive_type.trim();
    if let Some(inner) = s
        .strip_prefix("decimal(")
        .or_else(|| s.strip_prefix("DECIMAL("))
        && let Some(inner) = inner.strip_suffix(')')
    {
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() == 2 {
            let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                DataFusionError::Internal(format!("invalid decimal precision in: {}", hive_type))
            })?;
            let scale = parts[1].trim().parse::<i8>().map_err(|_| {
                DataFusionError::Internal(format!("invalid decimal scale in: {}", hive_type))
            })?;
            return Ok(DataType::Decimal128(precision, scale));
        }
    }
    Ok(DataType::Decimal128(38, 10))
}
