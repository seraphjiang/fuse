// SPDX-License-Identifier: Apache-2.0
//! Query execution pipeline — composable stages for query processing.

/// Pipeline stage result.
#[derive(Debug)]
pub enum StageResult {
    Continue(String),     // Pass modified query to next stage
    ShortCircuit(String), // Return early with this response
    Error(String),        // Abort with error
}

/// A pipeline stage that processes a query.
pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &str;
    fn process(&self, query: &str) -> StageResult;
}

/// Execute a query through a pipeline of stages.
pub fn run_pipeline(query: &str, stages: &[Box<dyn PipelineStage>]) -> Result<String, String> {
    let mut q = query.to_string();
    for stage in stages {
        match stage.process(&q) {
            StageResult::Continue(next) => q = next,
            StageResult::ShortCircuit(resp) => return Ok(resp),
            StageResult::Error(e) => return Err(format!("{}: {}", stage.name(), e)),
        }
    }
    Ok(q)
}

/// Whitespace normalization stage.
pub struct NormalizeStage;
impl PipelineStage for NormalizeStage {
    fn name(&self) -> &str {
        "normalize"
    }
    fn process(&self, query: &str) -> StageResult {
        StageResult::Continue(query.split_whitespace().collect::<Vec<_>>().join(" "))
    }
}

/// Empty query rejection stage.
pub struct RejectEmptyStage;
impl PipelineStage for RejectEmptyStage {
    fn name(&self) -> &str {
        "reject_empty"
    }
    fn process(&self, query: &str) -> StageResult {
        if query.trim().is_empty() {
            StageResult::Error("query cannot be empty".into())
        } else {
            StageResult::Continue(query.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_continue() {
        let stages: Vec<Box<dyn PipelineStage>> = vec![Box::new(NormalizeStage)];
        let result = run_pipeline("SELECT  *  FROM  t", &stages).unwrap();
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn test_pipeline_error() {
        let stages: Vec<Box<dyn PipelineStage>> = vec![Box::new(RejectEmptyStage)];
        assert!(run_pipeline("", &stages).is_err());
    }

    #[test]
    fn test_pipeline_chain() {
        let stages: Vec<Box<dyn PipelineStage>> =
            vec![Box::new(NormalizeStage), Box::new(RejectEmptyStage)];
        assert!(run_pipeline("SELECT 1", &stages).is_ok());
    }

    #[test]
    fn test_short_circuit() {
        struct CacheHit;
        impl PipelineStage for CacheHit {
            fn name(&self) -> &str {
                "cache"
            }
            fn process(&self, _: &str) -> StageResult {
                StageResult::ShortCircuit("cached result".into())
            }
        }
        let stages: Vec<Box<dyn PipelineStage>> = vec![Box::new(CacheHit)];
        assert_eq!(run_pipeline("q", &stages).unwrap(), "cached result");
    }
}
