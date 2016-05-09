package workflow.graph

/**
 * Optimizes a Pipeline DAG
 */
object DefaultOptimizer extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
      Nil
}

/**
 * Merges equivalent nodes in the DAG.
 * Nodes are considered equivalent if:
 * - The operators stored within the nodes are equal (.equals() is true)
 * - They share the same dependencies
 */
object EquivalentNodeMergeRule extends Rule {
  override def apply(plan: Graph): Graph = {
    val nodeSetsToMerge = plan.nodes.groupBy(id => (plan.getOperator(id), plan.getDependencies(id))).values

    if (nodeSetsToMerge.size == plan.nodes.size) {
      // no nodes are mergable
      plan
    } else {
      nodeSetsToMerge.filter(_.size > 1).foldLeft(plan) {
        case (curPlan, setToMerge) => {
          val nodeToKeep = setToMerge.minBy(_.id)
          (setToMerge - nodeToKeep).foldLeft(curPlan) {
            case (partialMergedPlan, nodeToMerge) => {
              partialMergedPlan
                .replaceDependency(nodeToMerge, nodeToKeep)
                .removeNode(nodeToMerge)
            }
          }
        }
      }
    }
  }
}