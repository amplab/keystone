package workflow.graph

/**
 * A rule to merge equivalent nodes in the DAG.
 * Nodes are considered equivalent if:
 * - The operators stored within the nodes are equal, i.e. `.hashCode()` of both ops is equal AND `.equals()` is true
 *    (when an operator is a case class, both methods will automatically be generated)
 * - They share the same dependencies
 *
 * This rule also merges prefixes if any of
 * the nodes being merged have their prefix attached.
 */
object EquivalentNodeMergeRule extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val nodeSetsToMerge = plan.nodes.groupBy(id => (plan.getOperator(id), plan.getDependencies(id))).values

    if (nodeSetsToMerge.size == plan.nodes.size) {
      // no nodes are mergable
      (plan, prefixes)
    } else {
      nodeSetsToMerge.filter(_.size > 1).foldLeft((plan, prefixes)) {
        case ((curPlan, curPrefixes), setToMerge) => {
          // Construct a graph that merges all of the nodes
          val nodeToKeep = setToMerge.minBy(_.id)
          val nextGraph = (setToMerge - nodeToKeep).foldLeft(curPlan) {
            case (partialMergedPlan, nodeToMerge) => {
              partialMergedPlan
                .replaceDependency(nodeToMerge, nodeToKeep)
                .removeNode(nodeToMerge)
            }
          }

          // If any of the nodes being merged have been executed, update the prefixes
          val prefix = setToMerge.collectFirst {
            case node if curPrefixes.contains(node) => curPrefixes(node)
          }
          val nextPrefixes = if (prefix.nonEmpty) {
            (curPrefixes -- setToMerge) + (nodeToKeep -> prefix.get)
          } else {
            curPrefixes
          }

          (nextGraph, nextPrefixes)
        }
      }
    }
  }
}
