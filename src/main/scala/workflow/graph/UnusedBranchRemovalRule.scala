package workflow.graph

/**
 * A rule to remove all nodes & sources in a graph that don't lead to any sink.
 */
object UnusedBranchRemovalRule extends Rule {
  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val ancestorsOfSinks = plan.sinks.foldLeft(Set[GraphId]()) {
      case (ancestors, sink) => ancestors ++ AnalysisUtils.getAncestors(plan, sink)
    }

    val nodesToRemove = plan.nodes -- ancestorsOfSinks.collect { case node: NodeId => node }
    val sourcesToRemove = plan.sources -- ancestorsOfSinks.collect { case source: SourceId => source }

    val afterSourceRemoval = sourcesToRemove.foldLeft(plan) {
      case (curPlan, sourceToRemove) => curPlan.removeSource(sourceToRemove)
    }

    nodesToRemove.foldLeft((afterSourceRemoval, prefixes)) {
      case ((curPlan, curPrefixes), nodeToRemove) => (curPlan.removeNode(nodeToRemove), curPrefixes - nodeToRemove)
    }
  }
}
