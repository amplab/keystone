package workflow.graph

/**
 * A rule to merge equivalent nodes in the DAG.
 * Nodes are considered equivalent if:
 * - The operators stored within the nodes are equal (.equals() is true)
 * - They share the same dependencies
 *
 * This rule also merges execution state if any of
 * the nodes being merged already have state attached.
 */
object EquivalentNodeMergeRule extends Rule {
  override def apply(plan: Graph, executionState: Map[GraphId, Expression]): (Graph, Map[GraphId, Expression]) = {
    val nodeSetsToMerge = plan.nodes.groupBy(id => (plan.getOperator(id), plan.getDependencies(id))).values

    if (nodeSetsToMerge.size == plan.nodes.size) {
      // no nodes are mergable
      (plan, executionState)
    } else {
      nodeSetsToMerge.filter(_.size > 1).foldLeft((plan, executionState)) {
        case ((curPlan, curExecutionState), setToMerge) => {
          // Construct a graph that merges all of the nodes
          val nodeToKeep = setToMerge.minBy(_.id)
          val nextGraph = (setToMerge - nodeToKeep).foldLeft(curPlan) {
            case (partialMergedPlan, nodeToMerge) => {
              partialMergedPlan
                .replaceDependency(nodeToMerge, nodeToKeep)
                .removeNode(nodeToMerge)
            }
          }

          // If any of the nodes being merged have been executed, update the execution state
          val executionValue = setToMerge.collectFirst {
            case node if curExecutionState.contains(node) => curExecutionState(node)
          }
          val nextExecutionState = if (executionValue.nonEmpty) {
            (curExecutionState -- setToMerge) + (nodeToKeep -> executionValue.get)
          } else {
            curExecutionState
          }

          (nextGraph, nextExecutionState)
        }
      }
    }
  }
}

/**
 * An optimizer that merges all equivalent nodes in a Pipeline DAG.
 * It is used by the incremental Pipeline construction & execution methods.
 */
object EquivalentNodeMergeOptimizer extends Optimizer {
  protected val batches: Seq[Batch] =
    Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
      Nil
}
