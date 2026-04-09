use fuse_core::connector::{FilterExpr, ComparisonOp, ScalarValue, AggregationExpr, AggFunction, SortExpr, SubQuery};

/// Translate a SubQuery into an OpenSearch JSON query body.
pub fn translate_to_query_dsl(query: &SubQuery) -> serde_json::Value {
    let mut body = serde_json::Map::new();

    // Projections → _source
    if !query.projections.is_empty() {
        body.insert("_source".into(), serde_json::json!(&query.projections));
    }

    // Filter → query
    if let Some(filter) = &query.filter {
        body.insert("query".into(), translate_filter(filter));
    } else {
        body.insert("query".into(), serde_json::json!({"match_all": {}}));
    }

    // Aggregations
    if !query.aggregations.is_empty() {
        body.insert("aggs".into(), translate_aggregations(&query.aggregations, &query.group_by));
        body.insert("size".into(), serde_json::json!(0));
    } else {
        // Sort (only when not aggregating)
        if !query.sort.is_empty() {
            body.insert("sort".into(), translate_sort(&query.sort));
        }
        // Limit
        if let Some(limit) = query.limit {
            body.insert("size".into(), serde_json::json!(limit));
        }
    }

    // Passthrough: merge raw JSON into body
    if let Some(serde_json::Value::Object(extra)) = &query.passthrough {
        for (k, v) in extra {
            body.insert(k.clone(), v.clone());
        }
    }

    serde_json::Value::Object(body)
}

fn translate_filter(expr: &FilterExpr) -> serde_json::Value {
    match expr {
        FilterExpr::And(left, right) => {
            serde_json::json!({
                "bool": {
                    "must": [translate_filter(left), translate_filter(right)]
                }
            })
        }
        FilterExpr::Or(left, right) => {
            serde_json::json!({
                "bool": {
                    "should": [translate_filter(left), translate_filter(right)],
                    "minimum_should_match": 1
                }
            })
        }
        FilterExpr::Not(inner) => {
            serde_json::json!({
                "bool": {
                    "must_not": [translate_filter(inner)]
                }
            })
        }
        FilterExpr::Comparison { field, op, value } => translate_comparison(field, *op, value),
        FilterExpr::In { field, values } => {
            let vals: Vec<serde_json::Value> = values.iter().map(scalar_to_json).collect();
            serde_json::json!({"terms": {field: vals}})
        }
        FilterExpr::IsNull(field) => {
            serde_json::json!({"bool": {"must_not": [{"exists": {"field": field}}]}})
        }
        FilterExpr::IsNotNull(field) => {
            serde_json::json!({"exists": {"field": field}})
        }
    }
}

fn translate_comparison(field: &str, op: ComparisonOp, value: &ScalarValue) -> serde_json::Value {
    let val = scalar_to_json(value);
    match op {
        ComparisonOp::Eq => serde_json::json!({"term": {field: val}}),
        ComparisonOp::Neq => {
            serde_json::json!({"bool": {"must_not": [{"term": {field: val}}]}})
        }
        ComparisonOp::Lt => serde_json::json!({"range": {field: {"lt": val}}}),
        ComparisonOp::Lte => serde_json::json!({"range": {field: {"lte": val}}}),
        ComparisonOp::Gt => serde_json::json!({"range": {field: {"gt": val}}}),
        ComparisonOp::Gte => serde_json::json!({"range": {field: {"gte": val}}}),
        ComparisonOp::Like => {
            // Map LIKE to wildcard query
            if let ScalarValue::Utf8(pattern) = value {
                let wildcard = pattern.replace('%', "*").replace('_', "?");
                serde_json::json!({"wildcard": {field: {"value": wildcard}}})
            } else {
                serde_json::json!({"match_all": {}})
            }
        }
    }
}

fn translate_aggregations(aggs: &[AggregationExpr], group_by: &[String]) -> serde_json::Value {
    let mut aggs_map = serde_json::Map::new();

    if group_by.is_empty() {
        // Metric aggregations only
        for agg in aggs {
            aggs_map.insert(agg.alias.clone(), translate_single_agg(agg));
        }
    } else {
        // Composite/terms aggregation for group-by, with sub-aggregations
        let group_field = &group_by[0]; // simplified: single group-by field
        let mut sub_aggs = serde_json::Map::new();
        for agg in aggs {
            sub_aggs.insert(agg.alias.clone(), translate_single_agg(agg));
        }
        aggs_map.insert(
            "group_by".into(),
            serde_json::json!({
                "terms": {"field": group_field, "size": 10000},
                "aggs": serde_json::Value::Object(sub_aggs)
            }),
        );
    }

    serde_json::Value::Object(aggs_map)
}

fn translate_single_agg(agg: &AggregationExpr) -> serde_json::Value {
    match agg.function {
        AggFunction::Count => {
            if let Some(field) = &agg.field {
                serde_json::json!({"value_count": {"field": field}})
            } else {
                // count(*) — use _count from bucket or value_count on _id
                serde_json::json!({"value_count": {"field": "_id"}})
            }
        }
        AggFunction::Sum => serde_json::json!({"sum": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Avg => serde_json::json!({"avg": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Min => serde_json::json!({"min": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Max => serde_json::json!({"max": {"field": agg.field.as_deref().unwrap_or("_id")}}),
    }
}

fn translate_sort(sort: &[SortExpr]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = sort
        .iter()
        .map(|s| {
            let order = if s.descending { "desc" } else { "asc" };
            serde_json::json!({&s.field: {"order": order}})
        })
        .collect();
    serde_json::json!(items)
}

fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(b) => serde_json::json!(b),
        ScalarValue::Int64(n) => serde_json::json!(n),
        ScalarValue::Float64(f) => serde_json::json!(f),
        ScalarValue::Utf8(s) => serde_json::json!(s),
    }
}
