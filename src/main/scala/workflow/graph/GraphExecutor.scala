package workflow.graph

/**
 * A GraphExecutor is constructed using a graph with already-executed state attached to it.
 * It provides methods to further execute parts of the graph, returning the execution result.
 * By default, it will optimize the graph before any new execution occurs.
 *
 * Warning: Not thread-safe!
 *
 * @param graph The underlying graph to execute
 * @param state A mapping of GraphId to computed Expression.
 *              This is the current state of what has already been executed in the graph.
 * @param optimize Whether or not to optimize the graph using the global executor before executing anything.
 *                 Defaults to True.
 */
private[graph] class GraphExecutor(graph: Graph, optimize: Boolean = true) {
  // internally used flag to check if optimization has occurred yet. Not thread-safe!
  private var optimized: Boolean = false

  // The lazily computed result of optimizing the initial graph w/ initial state attached to it
  private lazy val optimizedGraphAndState: (Graph, Map[GraphId, Expression]) = {
    optimized = true

    if (optimize) {
      Pipeline.getOptimizer.execute(graph, state)
    } else {
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
   * This is the optimized graph if optimization has occurred, otherwise
   * it is the initial graph this executor was constructed with.
   */
  def getGraph: Graph = if (optimized) {
    optimizedGraph
  } else {
    graph
  }

  /**
   * Partially execute a graph up to an input GraphId as far as is possible.
   * No node will be executed that depends on unconnected sources, but ancestors the input id
   * that do not depend on sources may be executed.
   *
   * This method updates the internal execution state of the GraphExecutor.
   *
   * @param graphId The GraphId to execute up to and including if possible.
   */
  def partialExecute(graphId: GraphId): Unit = {
    val linearization = AnalysisUtils.linearize(optimizedGraph, graphId) :+ graphId
    val idsToExecute = linearization.filter(id => !sourceDependants.contains(id))
    idsToExecute.foreach(execute)
  }

  /**
   * Execute the graph up to and including an input graph id, and return the result
   * of execution at that id.
   * This method updates the internal execution state of the GraphExecutor.
   *
   * @param graphId The GraphId to execute up to and including.
   * @return The execution result at the input graph id.
   */
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
