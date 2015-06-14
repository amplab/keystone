package workflow

/**
 * Optimizes a Pipeline DAG
 */
object Optimizer extends RuleExecutor {
  protected val batches: Seq[Batch] = Batch("DAG Optimization", FixedPoint(100), EquivalentNodeMerger) :: Nil
}

/**
 * Merges equivalent node indices in the DAG.
 * Nodes are considered equivalent if:
 * - The nodes at the indices themselves are equal
 * - They point to the same data dependencies
 * - They point to the same fit dependencies
 */
object EquivalentNodeMerger extends Rule {
  def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val fullNodes = plan.nodes.zip(plan.dataDeps.zip(plan.fitDeps)).zipWithIndex
    val mergableNodes = fullNodes.groupBy(_._1).mapValues(_.map(_._2)).toSeq

    if (mergableNodes.size == plan.nodes.size) {
      // no nodes are mergable
      plan
    } else {
      val oldToNewNodeMapping = mergableNodes.zipWithIndex.map(x => x._1._2.map(y => (y, x._2))).flatMap(identity).toMap + (Pipeline.SOURCE -> Pipeline.SOURCE)
      val newNodes = mergableNodes.map(_._1._1)
      val newDataDeps = mergableNodes.map(_._1._2._1.map(x => oldToNewNodeMapping(x)))
      val newFitDeps = mergableNodes.map(_._1._2._2.map(x => oldToNewNodeMapping(x)))
      val newSink = oldToNewNodeMapping(plan.sink)

      Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
    }
  }
}