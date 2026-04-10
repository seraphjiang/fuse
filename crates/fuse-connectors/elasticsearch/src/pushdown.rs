// SPDX-License-Identifier: Apache-2.0

//! Query DSL translation for Elasticsearch — identical to OpenSearch DSL.
//! Re-exports the OpenSearch pushdown logic with no changes needed.

use fuse_core::connector::{AggFunction, ComparisonOp, FilterExpr, ScalarValue, SubQuery};

/// Translate a SubQuery into an Elasticsearch JSON query body.
pub fn translate_to_query_dsl(query: &SubQuery) -> serde_json::Value {
    let mut body = serde_json::Map::new();

    if !query.projections.is_empty() {
        body.insert("_source".into(), serde_json::json!(&query.projections));
    }

    if let Some(filter) = &query.filter {
        body.insert("query".into(), translate_filter(filter));
    } else {
        body.insert("query".into(), serde_json::json!({"match_all": {}}));
    }

    if !query.aggregations.is_empty() {
        body.insert("aggs".into(), translate_aggs(&query.aggregations, &query.group_by));
        body.insert("size".into(), serde_json::json!(0));
    } else {
        if !query.sort.is_empty() {
            let sort: Vec<serde_json::Value> = query.sort.iter().map(|s| {
                serde_json::json!({&s.field: {"order": if s.descending {"desc"} else {"asc"}}})
            }).collect();
            body.insert("sort".into(), serde_json::json!(sort));
        }
        if let Some(limit) = query.limit {
            body.insert("size".into(), serde_json::json!(limit));
        }
    }

    serde_json::Value::Object(body)
}

fn translate_filter(expr: &FilterExpr) -> serde_json::Value {
    match expr {
        FilterExpr::And(l, r) => serde_json::json!({"bool": {"must": [translate_filter(l), translate_filter(r)]}}),
        FilterExpr::Or(l, r) => serde_json::json!({"bool": {"should": [translate_filter(l), translate_filter(r)], "minimum_should_match": 1}}),
        FilterExpr::Not(inner) => serde_json::json!({"bool": {"must_not": [translate_filter(inner)]}}),
        FilterExpr::Comparison { field, op, value } => {
            let v = scalar_to_json(value);
            match op {
                ComparisonOp::Eq => serde_json::json!({"term": {field: v}}),
                ComparisonOp::Neq => serde_json::json!({"bool": {"must_not": [{"term": {field: v}}]}}),
                ComparisonOp::Lt => serde_json::json!({"range": {field: {"lt": v}}}),
                ComparisonOp::Lte => serde_json::json!({"range": {field: {"lte": v}}}),
                ComparisonOp::Gt => serde_json::json!({"range": {field: {"gt": v}}}),
                ComparisonOp::Gte => serde_json::json!({"range": {field: {"gte": v}}}),
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => {
                    let pattern = scalar_to_wildcard(value);
                    serde_json::json!({"wildcard": {field: {"value": pattern, "case_insensitive": true}}})
                }
            }
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<serde_json::Value> = values.iter().map(scalar_to_json).collect();
            serde_json::json!({"terms": {field: vals}})
        }
        FilterExpr::IsNull(field) => serde_json::json!({"bool": {"must_not": [{"exists": {"field": field}}]}}),
        FilterExpr::IsNotNull(field) => serde_json::json!({"exists": {"field": field}}),
    }
}

fn translate_aggs(aggs: &[fuse_core::connector::AggregationExpr], group_by: &[String]) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    if let Some(gb) = group_by.first() {
        let mut inner = serde_json::Map::new();
        for agg in aggs {
            let field = agg.field.as_deref().unwrap_or("_id");
            let agg_body = match agg.function {
                AggFunction::Count | AggFunction::CountDistinct => serde_json::json!({"value_count": {"field": field}}),
                AggFunction::Sum => serde_json::json!({"sum": {"field": field}}),
                AggFunction::Avg => serde_json::json!({"avg": {"field": field}}),
                AggFunction::Min => serde_json::json!({"min": {"field": field}}),
                AggFunction::Max => serde_json::json!({"max": {"field": field}}),
            };
            inner.insert(agg.alias.clone(), agg_body);
        }
        result.insert(gb.clone(), serde_json::json!({
            "terms": {"field": gb},
            "aggs": inner
        }));
    } else {
        for agg in aggs {
            let field = agg.field.as_deref().unwrap_or("_id");
            let agg_body = match agg.function {
                AggFunction::Count | AggFunction::CountDistinct => serde_json::json!({"value_count": {"field": field}}),
                AggFunction::Sum => serde_json::json!({"sum": {"field": field}}),
                AggFunction::Avg => serde_json::json!({"avg": {"field": field}}),
                AggFunction::Min => serde_json::json!({"min": {"field": field}}),
                AggFunction::Max => serde_json::json!({"max": {"field": field}}),
            };
            result.insert(agg.alias.clone(), agg_body);
        }
    }
    serde_json::Value::Object(result)
}

fn scalar_to_json(v: &ScalarValue) -> serde_json::Value {
    match v {
        ScalarValue::Utf8(s) => serde_json::Value::String(s.clone()),
        ScalarValue::Int64(n) => serde_json::json!(n),
        ScalarValue::Float64(f) => serde_json::json!(f),
        ScalarValue::Boolean(b) => serde_json::json!(b),
        ScalarValue::Null => serde_json::Value::Null,
    }
}

fn scalar_to_wildcard(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(s) => s.replace('%', "*").replace('_', "?"),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Null => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::{AggFunction, AggregationExpr, SortExpr, SubQuery};

    fn base() -> SubQuery {
        SubQuery { table: "logs".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None }
    }

    #[test]
    fn test_match_all_default() {
        let dsl = translate_to_query_dsl(&base());
        assert_eq!(dsl["query"], serde_json::json!({"match_all": {}}));
    }

    #[test]
    fn test_limit_sets_size() {
        let q = SubQuery { limit: Some(20), ..base() };
        assert_eq!(translate_to_query_dsl(&q)["size"], 20);
    }

    #[test]
    fn test_projections_set_source() {
        let q = SubQuery { projections: vec!["a".into(), "b".into()], ..base() };
        assert_eq!(translate_to_query_dsl(&q)["_source"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn test_eq_filter() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(200) }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["term"].is_object());
    }

    #[test]
    fn test_range_filter() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison { field: "age".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(18) }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["range"].is_object());
    }

    #[test]
    fn test_in_filter() {
        let q = SubQuery {
            filter: Some(FilterExpr::In { field: "env".into(), values: vec![ScalarValue::Utf8("prod".into()), ScalarValue::Utf8("staging".into())] }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["terms"].is_object());
    }

    #[test]
    fn test_sort() {
        let q = SubQuery {
            sort: vec![SortExpr { field: "ts".into(), descending: true }],
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["sort"].is_array());
    }

    #[test]
    fn test_count_agg() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["size"], 0);
        assert!(dsl["aggs"].is_object());
    }

    // ── #302 Verification tests (tester) ──

    #[test]
    fn test_like_to_wildcard() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "name".into(),
                op: ComparisonOp::Like,
                value: ScalarValue::Utf8("%alice_".into()),
            }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        let wc = &dsl["query"]["wildcard"]["name"]["value"];
        assert_eq!(wc, "*alice?", "% → *, _ → ?: got {}", wc);
    }

    #[test]
    fn test_ilike_to_wildcard_case_insensitive() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "host".into(),
                op: ComparisonOp::ILike,
                value: ScalarValue::Utf8("%prod%".into()),
            }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["wildcard"]["host"]["value"], "*prod*");
        assert_eq!(dsl["query"]["wildcard"]["host"]["case_insensitive"], true);
    }

    #[test]
    fn test_and_filter_bool_must() {
        let q = SubQuery {
            filter: Some(FilterExpr::And(
                Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
                Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(2) }),
            )),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must"].is_array());
        assert_eq!(dsl["query"]["bool"]["must"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_or_filter_bool_should() {
        let q = SubQuery {
            filter: Some(FilterExpr::Or(
                Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
                Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(2) }),
            )),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["should"].is_array());
        assert_eq!(dsl["query"]["bool"]["minimum_should_match"], 1);
    }

    #[test]
    fn test_not_filter_must_not() {
        let q = SubQuery {
            filter: Some(FilterExpr::Not(Box::new(
                FilterExpr::Comparison { field: "x".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(0) },
            ))),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"].is_array());
    }

    #[test]
    fn test_is_null_no_exists() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNull("email".into())),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"][0]["exists"].is_object());
    }

    #[test]
    fn test_is_not_null_exists() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNotNull("email".into())),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert_eq!(dsl["query"]["exists"]["field"], "email");
    }

    #[test]
    fn test_neq_uses_must_not_term() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "status".into(), op: ComparisonOp::Neq, value: ScalarValue::Int64(500),
            }),
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["query"]["bool"]["must_not"][0]["term"].is_object());
    }

    #[test]
    fn test_group_by_terms_agg() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Sum, field: Some("amount".into()), alias: "total".into() }],
            group_by: vec!["region".into()],
            ..base()
        };
        let dsl = translate_to_query_dsl(&q);
        assert!(dsl["aggs"]["region"]["terms"].is_object());
        assert!(dsl["aggs"]["region"]["aggs"]["total"]["sum"].is_object());
    }
}
