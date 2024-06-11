// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_expr_visitor::InputRefCounter;
use crate::optimizer::plan_node::{generic, PlanTreeNodeUnary, StreamProject};
use crate::optimizer::{BoxedRule, PlanRef, Rule};
use crate::utils::Substitute;

/// Merge contiguous [`StreamProject`] nodes.
pub struct StreamProjectMergeRule {}
impl Rule for StreamProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer_project = plan.as_stream_project()?;
        let input = outer_project.input();
        let inner_project = input.as_stream_project()?;

        let mut input_ref_counter = InputRefCounter::default();
        for expr in outer_project.exprs() {
            input_ref_counter.visit_expr(expr);
        }
        // bail out if it is a project generated by `CommonSubExprExtractRule`.
        for (index, count) in &input_ref_counter.counter {
            if *count > 1 && matches!(inner_project.exprs()[*index], ExprImpl::FunctionCall(_)) {
                return None;
            }
        }

        let mut subst = Substitute {
            mapping: inner_project.exprs().clone(),
        };
        let exprs = outer_project
            .exprs()
            .iter()
            .cloned()
            .map(|expr| subst.rewrite_expr(expr))
            .collect();
        let logical_project = generic::Project::new(exprs, inner_project.input());

        // If either of the projects has the hint, we should keep it.
        let noop_update_hint = outer_project.noop_update_hint() || inner_project.noop_update_hint();

        Some(
            StreamProject::new(logical_project)
                .with_noop_update_hint(noop_update_hint)
                .into(),
        )
    }
}

impl StreamProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(StreamProjectMergeRule {})
    }
}
