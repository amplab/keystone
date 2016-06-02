package workflow.graph

import workflow.graph.AutoCacheRule.GreedyCache

/**
 * The default Pipeline optimizer used when executing pipelines.
 */
object DefaultOptimizer extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("Load Saved State", Once, ExtractSaveablePrefixes, SavedStateLoadRule, UnusedBranchRemovalRule) ::
    Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
    Batch("Node Level Optimization", Once, new NodeOptimizationRule) ::
      Nil
}

/**
 * Optimizes a Pipeline DAG, with auto-caching
 */
class AutoCachingOptimizer(strategy: AutoCacheRule.CachingStrategy = GreedyCache()) extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("Load Saved State", Once, ExtractSaveablePrefixes, SavedStateLoadRule, UnusedBranchRemovalRule) ::
    Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
    Batch("Node Level Optimization", Once, new NodeOptimizationRule) ::
    Batch("Auto Cache", Once, new AutoCacheRule(strategy)) ::
    Nil
}

