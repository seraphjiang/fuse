// SPDX-License-Identifier: Apache-2.0
//! Plan cost comparator — choose the cheapest execution plan.

use crate::cost_model::CostEstimate;

/// Compare two plans and return the cheaper one's index (0 or 1).
pub fn cheaper(a: &CostEstimate, b: &CostEstimate) -> usize {
    if a.estimated_cost <= b.estimated_cost {
        0
    } else {
        1
    }
}

/// Pick the cheapest plan from a list. Returns index.
pub fn cheapest(plans: &[CostEstimate]) -> Option<usize> {
    plans
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.estimated_cost
                .partial_cmp(&b.estimated_cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(i, _)| i)
}

/// Check if plan B is significantly cheaper than A (by ratio).
pub fn is_significantly_cheaper(a: &CostEstimate, b: &CostEstimate, threshold: f64) -> bool {
    if a.estimated_cost == 0.0 {
        return false;
    }
    b.estimated_cost / a.estimated_cost < threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model;

    #[test]
    fn test_cheaper() {
        let a = cost_model::scan_cost(1000, 5);
        let b = cost_model::scan_cost(100, 5);
        assert_eq!(cheaper(&a, &b), 1); // b is cheaper
    }

    #[test]
    fn test_cheapest() {
        let plans = vec![
            cost_model::scan_cost(1000, 5),
            cost_model::scan_cost(100, 5),
            cost_model::scan_cost(500, 5),
        ];
        assert_eq!(cheapest(&plans), Some(1));
    }

    #[test]
    fn test_cheapest_empty() {
        assert_eq!(cheapest(&[]), None);
    }

    #[test]
    fn test_significantly_cheaper() {
        let a = cost_model::scan_cost(10000, 10);
        let b = cost_model::scan_cost(100, 10);
        assert!(is_significantly_cheaper(&a, &b, 0.5));
        assert!(!is_significantly_cheaper(&b, &a, 0.5));
    }
}
