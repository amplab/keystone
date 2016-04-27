package internal

object AnalysisUtils {
  // util to get ancestors as tree?
  // util to topologically sort ancestors?
  // FIXME: Linearize won't be deterministic currently
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

  /**
   * Given a graph and a source/sink/node, output the set of all sources/sinks/nodes
   * that directly depend on the source/sink/node.
   *
   * i.e. All ids with a direct dependency on this source/sink/node.
   *
   * @param graph The graph to use
   * @param id The node/sink/source in the graph
   * @return The set of all children of that id
   */
  def getChildren(graph: Graph, id: GraphId): Set[GraphId] = {
    id match {
      case id: NodeOrSourceId => {
        val childrenNodes = graph.dependencies.filter(_._2.contains(id)).keySet
        val childrenSinks = graph.sinkDependencies.filter(_._2 == id).keySet
        childrenNodes ++ childrenSinks
      }
      case sinkId: SinkId => Set()
    }
  }

  /**
   * Given a graph and a source/sink/node, output the set of all sources/sinks/nodes
   * that have that source/sink/node as an ancestor.
   *
   * i.e. All ids that explicitly or implicitly depend on this source/sink/node.
   *
   * @param graph The graph to use
   * @param id The node/sink/source in the graph
   * @return The set of all descendents of that id
   */
  def getDescendants(graph: Graph, id: GraphId): Set[GraphId] = {
    val children = getChildren(graph, id)
    children.map {
      child => getDescendants(graph, child) + child
    }.fold(Set())(_ union _)
  }

  /**
   * Given a graph and a source/sink/node, output the set of all nodes
   * and sources that are parents of that source/sink/node in the graph.
   *
   * i.e. All ids that source/sink/node has as a dependency
   *
   * @param graph The graph to use
   * @param id A node/sink/source in the graph
   * @return The set of all parents of that id
   */
  def getParents(graph: Graph, id: GraphId): Set[NodeOrSourceId] = {
    id match {
      case source: SourceId => Set()
      case node: NodeId => graph.getDependencies(node).toSet
      case sink: SinkId => Set(graph.getSinkDependency(sink))
    }
  }

  /**
   * Given a graph and a source/sink/node, output the set of all nodes
   * and sources that are ancestors of that source/sink/node in the graph.
   *
   * i.e. All ids that source/sink/node has explicit or implicit dependencies on.
   *
   * @param graph The graph to use
   * @param id A node/sink/source in the graph
   * @return The set of all ancestors of that id
   */
  def getAncestors(graph: Graph, id: GraphId): Set[NodeOrSourceId] = {
    val parents = getParents(graph, id)
    parents.map {
      parent => getAncestors(graph, parent) + parent
    }.fold(Set())(_ union _)
  }
}
