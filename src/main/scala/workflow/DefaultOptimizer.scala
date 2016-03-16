package workflow

import workflow.AutoCacheRule.GreedyCache

/**
 * Optimizes a Pipeline DAG
 */
object DefaultOptimizer extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("DAG Optimization", FixedPoint(100), EquivalentNodeMergeRule) ::
    Batch("Node Level Optimization", Once, new NodeOptimizationRule) ::
      Nil
}

/**
 * Optimizes a Pipeline DAG, with auto-caching
 */
class AutoCachingOptimizer(strategy: AutoCacheRule.CachingStrategy = GreedyCache()) extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("DAG Optimization", FixedPoint(100), EquivalentNodeMergeRule) ::
    Batch("Node Level Optimization", Once, new NodeOptimizationRule) ::
    Batch("Auto Cache", Once, new AutoCacheRule(strategy)) ::
      Nil
}

/**
 * Merges equivalent node indices in the DAG.
 * Nodes are considered equivalent if:
 * - The nodes at the indices themselves are equal (.equals() is true)
 * - They point to the same data dependencies
 * - They point to the same fit dependencies
 */
object EquivalentNodeMergeRule extends Rule {
  def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val fullNodes = plan.nodes.zip(plan.dataDeps.zip(plan.fitDeps)).zipWithIndex
    val mergableNodes = fullNodes.groupBy(_._1).mapValues(_.map(_._2)).toSeq.sortBy(_._2.min)

    if (mergableNodes.size == plan.nodes.size) {
      // no nodes are mergable
      plan
    } else {
      val mergedNodeIdsWithOldIds = mergableNodes.zipWithIndex.map(x => (x._2, x._1._2))
      val oldToNewNodeMapping = mergedNodeIdsWithOldIds.flatMap(x => x._2.map(y => (y, x._1))).toMap +
          (Pipeline.SOURCE -> Pipeline.SOURCE)
      val newNodes = mergableNodes.map(_._1._1)
      val newDataDeps = mergableNodes.map(_._1._2._1.map(x => oldToNewNodeMapping(x)))
      val newFitDeps = mergableNodes.map(_._1._2._2.map(x => oldToNewNodeMapping(x)))
      val newSink = oldToNewNodeMapping(plan.sink)

      Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
    }
  }
}