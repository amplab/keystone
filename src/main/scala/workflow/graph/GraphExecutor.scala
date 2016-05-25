package workflow.graph

private[graph] class GraphExecutor(graph: Graph, state: Map[GraphId, Expression], optimize: Boolean = true) {
  private var optimized: Boolean = false
  private lazy val optimizedGraphAndState: (Graph, scala.collection.mutable.Map[GraphId, Expression]) = {
    val (newGraph, newState) = if (optimize) {
      Pipeline.getOptimizer.execute(graph, state)
    } else {
      (graph, state)
    }

    optimized = true
    (newGraph, scala.collection.mutable.Map() ++ newState)
  }
  private lazy val optimizedGraph: Graph = optimizedGraphAndState._1
  private lazy val optimizedState: scala.collection.mutable.Map[GraphId, Expression] = optimizedGraphAndState._2

  def getGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  def getState: Map[GraphId, Expression] = if (optimized) {
    optimizedState.toMap
  } else {
    state
  }

  private lazy val sourceDependants: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
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
        case source: SourceId => throw new IllegalArgumentException("Unconnected sources have no stored value")
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
