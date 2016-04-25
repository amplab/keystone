package internals

object AnalysisUtils {
  def linearize(graph: Graph): Seq[GraphId] = {
    def linearize(graphId: GraphId): Seq[GraphId] = {
      val deps: Seq[GraphId] = graphId match {
        case source: SourceId => Seq()
        case node: NodeId => graph.getDependencies(node)
        case sink: SinkId => Seq(graph.getSinkDependency(sink))
      }
      deps.foldLeft(Seq[GraphId]()) {
        case (linearization, dep) => if (!linearization.contains(dep)) {
          linearization ++ linearize(dep).filter(id => !linearization.contains(id))
        } else {
          linearization
        }
      } :+ graphId
    }

    graph.sinks.foldLeft(Seq[GraphId]()) {
      case (linearization, sink) => {
        linearization ++ linearize(sink).filter(id => !linearization.contains(id))
      }
    }
  }

  def getChildren(graph: Graph, node: GraphId): Set[GraphId] = {
    node match {
      case id: NodeOrSourceId => {
        val childrenNodes = graph.dependencies.filter(_._2.contains(id)).keySet
        val childrenSinks = graph.sinkDependencies.filter(_._2 == id).keySet
        childrenNodes ++ childrenSinks
      }
      case sinkId: SinkId => Set()
    }
  }

  def getDescendants(graph: Graph, node: GraphId): Set[GraphId] = {
    val children = getChildren(graph, node)
    children.map {
      child => getDescendants(graph, child) + child
    }.fold(Set())(_ union _)
  }

  def getParents(graph: Graph, node: GraphId): Set[NodeOrSourceId] = {
    node match {
      case source: SourceId => Set()
      case node: NodeId => graph.getDependencies(node).toSet
      case sink: SinkId => Set(graph.getSinkDependency(sink))
    }
  }

  def getAncestors(graph: Graph, node: GraphId): Set[NodeOrSourceId] = {
    val parents = getParents(graph, node)
    parents.map {
      parent => getAncestors(graph, parent) + parent
    }.fold(Set())(_ union _)
  }
}
