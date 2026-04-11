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
        ComparisonOp::ILike => {
            // Map ILIKE to case-insensitive wildcard query
            if let ScalarValue::Utf8(pattern) = value {
                let wildcard = pattern.replace('%', "*").replace('_', "?");
                serde_json::json!({"wildcard": {field: {"value": wildcard, "case_insensitive": true}}})
            } else {
                serde_json::json!({"match_all": {}})
            }
        }
        ComparisonOp::Contains => {
            // Native full-text match query
            serde_json::json!({"match": {field: {"query": val}}})
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
        AggFunction::CountDistinct | AggFunction::ApproxCountDistinct => {
            serde_json::json!({"cardinality": {"field": agg.field.as_deref().unwrap_or("_id")}})
        }
        AggFunction::Sum => serde_json::json!({"sum": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Avg => serde_json::json!({"avg": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Min => serde_json::json!({"min": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::Max => serde_json::json!({"max": {"field": agg.field.as_deref().unwrap_or("_id")}}),
        AggFunction::ApproxPercentile(p) => serde_json::json!({"percentiles": {"field": agg.field.as_deref().unwrap_or("_id"), "percents": [p]}}),
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


#[cfg(test)]
mod tests {
    use super::*;

    fn make_query() -> SubQuery {
        SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, passthrough: None, offset: None,
        }
    }

    #[test]
    fn test_match_all_when_no_filter() {
        let dsl = translate_to_query_dsl(&make_query());
        assert_eq!(dsl["query"], serde_json::json!({"match_all": {}}));
    }

    #[test]
    fn test_limit_becomes_size() {
        let mut q = make_query();
        q.limit = Some(25);
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 25);
    }

    #[test]
    fn test_projections_become_source() {
        let mut q = make_query();
        q.projections = vec!["service".into(), "status".into()];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["_source"], serde_json::json!(["service", "status"]));
    }

    #[test]
    fn test_equality_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Comparison {
            field: "service".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("api-gateway".into()),
        });
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"], serde_json::json!({"term": {"service": "api-gateway"}}));
    }

    #[test]
    fn test_range_filter_gte() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int64(500),
        });
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"], serde_json::json!({"range": {"status": {"gte": 500}}}));
    }

    #[test]
    fn test_and_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(500) }),
            Box::new(FilterExpr::Comparison { field: "service".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("auth".into()) }),
        ));
        let dsl = translate_to_query_dsl(&q);
        let must = &dsl["query"]["bool"]["must"];
        assert!(must.is_array());
        assert_eq!(must.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_or_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Or(
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(500) }),
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(503) }),
        ));
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["bool"]["minimum_should_match"], 1);
    }

    #[test]
    fn test_not_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Not(
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(200) }),
        ));
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"].is_array());
    }

    #[test]
    fn test_in_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::In {
            field: "status".into(),
            values: vec![ScalarValue::Int64(500), ScalarValue::Int64(502)],
        });
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["terms"]["status"], serde_json::json!([500, 502]));
    }

    #[test]
    fn test_is_null_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::IsNull("message".into()));
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"][0]["exists"]["field"].as_str() == Some("message"));
    }

    #[test]
    fn test_is_not_null_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::IsNotNull("message".into()));
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["exists"]["field"], "message");
    }

    #[test]
    fn test_like_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Comparison {
            field: "message".into(),
            op: ComparisonOp::Like,
            value: ScalarValue::Utf8("%timeout%".into()),
        });
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["wildcard"]["message"]["value"], "*timeout*");
    }

    #[test]
    fn test_sort() {
        let mut q = make_query();
        q.sort = vec![
            SortExpr { field: "timestamp".into(), descending: true },
            SortExpr { field: "status".into(), descending: false },
        ];
        let dsl = translate_to_query_dsl(&q);
        let sort = dsl["sort"].as_array().unwrap();
        assert_eq!(sort[0]["timestamp"]["order"], "desc");
        assert_eq!(sort[1]["status"]["order"], "asc");
    }

    #[test]
    fn test_count_aggregation_with_group_by() {
        let mut q = make_query();
        q.aggregations = vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }];
        q.group_by = vec!["service".into()];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        assert_eq!(dsl["aggs"]["group_by"]["terms"]["field"], "service");
    }

    #[test]
    fn test_avg_aggregation_no_group() {
        let mut q = make_query();
        q.aggregations = vec![AggregationExpr { function: AggFunction::Avg, field: Some("duration_ms".into()), alias: "avg_dur".into() }];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        assert_eq!(dsl["aggs"]["avg_dur"]["avg"]["field"], "duration_ms");
    }

    #[test]
    fn test_neq_filter() {
        let mut q = make_query();
        q.filter = Some(FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Neq,
            value: ScalarValue::Int64(200),
        });
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"][0]["term"]["status"].as_i64() == Some(200));
    }

    #[test]
    fn test_count_distinct_becomes_cardinality() {
        let mut q = make_query();
        q.aggregations = vec![AggregationExpr {
            function: AggFunction::CountDistinct,
            field: Some("user_id".into()),
            alias: "unique_users".into(),
        }];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        assert_eq!(dsl["aggs"]["unique_users"]["cardinality"]["field"], "user_id");
    }

    // ── COUNT DISTINCT verification (tester) ──

    #[test]
    fn test_count_distinct_with_group_by() {
        let mut q = make_query();
        q.group_by = vec!["service".into()];
        q.aggregations = vec![AggregationExpr {
            function: AggFunction::CountDistinct,
            field: Some("trace_id".into()),
            alias: "unique_traces".into(),
        }];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        // Should have group_by terms agg with nested cardinality
        assert!(dsl["aggs"]["group_by"].is_object());
    }

    #[test]
    fn test_count_distinct_no_field_defaults_to_id() {
        let mut q = make_query();
        q.aggregations = vec![AggregationExpr {
            function: AggFunction::CountDistinct,
            field: None,
            alias: "unique_docs".into(),
        }];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["aggs"]["unique_docs"]["cardinality"]["field"], "_id");
    }

    #[test]
    fn test_count_distinct_and_regular_count_together() {
        let mut q = make_query();
        q.aggregations = vec![
            AggregationExpr { function: AggFunction::Count, field: None, alias: "total".into() },
            AggregationExpr { function: AggFunction::CountDistinct, field: Some("host".into()), alias: "unique_hosts".into() },
        ];
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        assert!(dsl["aggs"]["total"].is_object());
        assert_eq!(dsl["aggs"]["unique_hosts"]["cardinality"]["field"], "host");
    }

    // ── #420 OpenSearch native match verification (tester) ──

    #[test]
    fn test_contains_uses_native_match() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "message".into(),
                op: ComparisonOp::Contains,
                value: ScalarValue::Utf8("OutOfMemory".into()),
            }),
            ..make_query()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["match"]["message"].is_object(),
            "Contains should use native match query: {:?}", dsl["query"]);
        assert_eq!(dsl["query"]["match"]["message"]["query"], "OutOfMemory");
    }
}
