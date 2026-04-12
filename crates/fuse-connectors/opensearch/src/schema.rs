use arrow::datatypes::{DataType, Field, Schema};
use fuse_core::ConnectorError;

/// Convert an OpenSearch index mapping JSON into an Arrow Schema.
///
/// Expects the mapping format returned by `GET /{index}/_mapping`:
/// `{ "index_name": { "mappings": { "properties": { ... } } } }`
pub fn mapping_to_arrow_schema(mapping_json: &serde_json::Value) -> Result<Schema, ConnectorError> {
    // Navigate to the properties object — handle both wrapped and unwrapped formats
    let properties = find_properties(mapping_json)
        .ok_or_else(|| ConnectorError::schema("no 'properties' found in mapping"))?;

    let fields = properties_to_fields(properties)?;
    Ok(Schema::new(fields))
}

fn find_properties(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    // Direct: {"properties": {...}}
    if let Some(props) = value.get("properties").and_then(|v| v.as_object()) {
        return Some(props);
    }
    // Wrapped: {"index": {"mappings": {"properties": {...}}}}
    if let Some(obj) = value.as_object() {
        for v in obj.values() {
            if let Some(props) = v
                .get("mappings")
                .and_then(|m| m.get("properties"))
                .and_then(|p| p.as_object())
            {
                return Some(props);
            }
        }
    }
    None
}

fn properties_to_fields(
    properties: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<Field>, ConnectorError> {
    let mut fields = Vec::new();
    for (name, def) in properties {
        let field = property_to_field(name, def)?;
        fields.push(field);
    }
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Ok(fields)
}

fn property_to_field(name: &str, def: &serde_json::Value) -> Result<Field, ConnectorError> {
    // Nested object with sub-properties
    if let Some(sub_props) = def.get("properties").and_then(|v| v.as_object()) {
        let sub_fields = properties_to_fields(sub_props)?;
        return Ok(Field::new(name, DataType::Struct(sub_fields.into()), true));
    }

    let os_type = def.get("type").and_then(|t| t.as_str()).unwrap_or("object");

    let arrow_type = os_type_to_arrow(os_type);
    Ok(Field::new(name, arrow_type, true))
}

/// Map OpenSearch field types to Arrow DataTypes.
fn os_type_to_arrow(os_type: &str) -> DataType {
    match os_type {
        "text" | "keyword" | "wildcard" => DataType::Utf8,
        "long" => DataType::Int64,
        "integer" => DataType::Int32,
        "short" => DataType::Int16,
        "byte" => DataType::Int8,
        "double" => DataType::Float64,
        "float" | "half_float" | "scaled_float" => DataType::Float32,
        "boolean" => DataType::Boolean,
        "date" | "date_nanos" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into()))
        }
        "binary" => DataType::Binary,
        "ip" => DataType::Utf8,
        "geo_point" | "geo_shape" => DataType::Utf8, // serialize as JSON string
        _ => DataType::Utf8,                         // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_mapping() {
        let mapping = json!({
            "properties": {
                "status": {"type": "integer"},
                "message": {"type": "text"},
                "timestamp": {"type": "date"}
            }
        });
        let schema = mapping_to_arrow_schema(&mapping).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(
            schema.field_with_name("status").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema.field_with_name("message").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_wrapped_mapping() {
        let mapping = json!({
            "my_index": {
                "mappings": {
                    "properties": {
                        "host": {"type": "keyword"}
                    }
                }
            }
        });
        let schema = mapping_to_arrow_schema(&mapping).unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(
            schema.field_with_name("host").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_nested_object() {
        let mapping = json!({
            "properties": {
                "user": {
                    "properties": {
                        "name": {"type": "keyword"},
                        "age": {"type": "integer"}
                    }
                }
            }
        });
        let schema = mapping_to_arrow_schema(&mapping).unwrap();
        let user_field = schema.field_with_name("user").unwrap();
        assert!(matches!(user_field.data_type(), DataType::Struct(_)));
    }

    #[test]
    fn test_no_properties_error() {
        let mapping = json!({"something": "else"});
        assert!(mapping_to_arrow_schema(&mapping).is_err());
    }

    #[test]
    fn test_type_mappings() {
        assert_eq!(os_type_to_arrow("long"), DataType::Int64);
        assert_eq!(os_type_to_arrow("double"), DataType::Float64);
        assert_eq!(os_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(os_type_to_arrow("keyword"), DataType::Utf8);
        assert_eq!(os_type_to_arrow("binary"), DataType::Binary);
        assert_eq!(os_type_to_arrow("ip"), DataType::Utf8);
        assert_eq!(os_type_to_arrow("unknown_type"), DataType::Utf8);
    }

    #[test]
    fn test_fields_sorted_alphabetically() {
        let mapping = json!({
            "properties": {
                "zebra": {"type": "keyword"},
                "alpha": {"type": "keyword"},
                "middle": {"type": "keyword"}
            }
        });
        let schema = mapping_to_arrow_schema(&mapping).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["alpha", "middle", "zebra"]);
    }
}
