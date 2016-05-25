package workflow.graph

// Note: NONE OF THIS $#@! is multithreading-safe!
private[graph] class GraphExecutor(graph: Graph, state: Map[GraphId, Expression], optimize: Boolean = true) {
  // internally used flag to check if optimization has occurred yet. Not thread-safe!
  private var optimized: Boolean = false

  // The lazily computed result of optimizing the initial graph w/ initial state attached to it
  private lazy val optimizedGraphAndState: (Graph, Map[GraphId, Expression]) = {
    if (optimize) {
      Pipeline.getOptimizer.execute(graph, state)
    } else {
      optimized = true
      (graph, state)
    }
  }

  // The optimized graph. Lazily computed, requires optimization.
  private lazy val optimizedGraph: Graph = optimizedGraphAndState._1

  // The mutable internal state attached to the optimized graph. Lazily computed, requires optimization.
  private lazy val optimizedState: scala.collection.mutable.Map[GraphId, Expression] =
    scala.collection.mutable.Map() ++ optimizedGraphAndState._2

  // Internally used set of all ids in the optimized graph that are source ids, or depend explicitly or implicitly
  // on sources. Lazily computed, requires optimization.
  private lazy val sourceDependants: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
  }

  /**
   * Get the current graph this executor is wrapping around.
   *
   * @return The optimized graph if it has already been optimized, otherwise
   *         the initial graph this executor was constructed with.
   */
  def getGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  /**
   * Get the current execution state of this executor.
   * @return An immutable copy of the current state
   */
  def getState: Map[GraphId, Expression] = if (optimized) {
    optimizedState.toMap
  } else {
    state
  }

  def partialExecute(graphId: GraphId): Unit = {
    val linearization = AnalysisUtils.linearize(optimizedGraph, graphId) :+ graphId
    val idsToExecute = linearization.filter(id => (!sourceDependants.contains(id)) && (!optimizedState.contains(id)))
    idsToExecute.foreach {ancestor => execute(ancestor)}
  }

  def execute(graphId: GraphId): Expression = {
    require(!sourceDependants.contains(graphId), "May not execute GraphIds that depend on unconnected sources.")

    optimizedState.getOrElseUpdate(graphId, {
      graphId match {
        case source: SourceId => throw new IllegalArgumentException("SourceIds may not be executed.")
        case node: NodeId => {
          val dependencies = optimizedGraph.getDependencies(node)
          val depExpressions = dependencies.map(dep => execute(dep))
          val operator = optimizedGraph.getOperator(node)
          operator.execute(depExpressions)
        }
        case sink: SinkId => {
          val sinkDep = optimizedGraph.getSinkDependency(sink)
          execute(sinkDep)
        }
      }
    })
  }
}
