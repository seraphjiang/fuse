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

fn find_properties(value: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
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
        return Ok(Field::new(
            name,
            DataType::Struct(sub_fields.into()),
            true,
        ));
    }

    let os_type = def
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("object");

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
        "date" | "date_nanos" => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into())),
        "binary" => DataType::Binary,
        "ip" => DataType::Utf8,
        "geo_point" | "geo_shape" => DataType::Utf8, // serialize as JSON string
        _ => DataType::Utf8, // fallback
    }
}
