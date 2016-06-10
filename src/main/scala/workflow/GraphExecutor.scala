package workflow

/**
 * A GraphExecutor is constructed using a graph.
 * It provides methods to execute parts of the graph, returning the execution result.
 * By default, it will optimize the graph before any new execution occurs.
 *
 * Warning: Not thread-safe!
 *
 * @param graph The underlying graph to execute
 * @param optimize Whether or not to optimize the graph using the global executor before executing anything.
 *                 Defaults to True.
 */
private[workflow] class GraphExecutor(val graph: Graph, val optimize: Boolean = true) {
  // internally used flag to check if optimization has occurred yet. Not thread-safe!
  private var optimized: Boolean = false

  // The lazily computed result of optimizing the initial graph w/ initial state attached to it
  private lazy val optimizedGraphAndPrefixes: (Graph, Map[NodeId, Prefix]) = {
    optimized = true

    if (optimize) {
      PipelineEnv.getOrCreate.getOptimizer.execute(graph, Map())
    } else {
      (graph, Map())
    }
  }

  // The optimized graph. Lazily computed, requires optimization.
  private lazy val optimizedGraph: Graph = optimizedGraphAndPrefixes._1
  private lazy val prefixes: Map[NodeId, Prefix] = optimizedGraphAndPrefixes._2

  // The mutable internal state attached to the optimized graph. Lazily computed, requires optimization.
  private val executionState: scala.collection.mutable.Map[GraphId, Expression] =
    scala.collection.mutable.Map()

  // Internally used set of all ids in the optimized graph that are source ids, or depend explicitly or implicitly
  // on sources. Lazily computed, requires optimization.
  private lazy val sourceDependants: Set[GraphId] = {
    optimizedGraph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(optimizedGraph, source) + source
    }
  }

  /**
   * Execute the graph up to and including an input graph id, and return the result
   * of execution at that id.
   * This method updates the [[PipelineEnv.state]] prefix state table.
   *
   * @param graphId The GraphId to execute up to and including.
   * @return The execution result at the input graph id.
   */
  def execute(graphId: GraphId): Expression = {
    require(!sourceDependants.contains(graphId), "May not execute GraphIds that depend on unconnected sources.")

    executionState.getOrElseUpdate(graphId, {
      graphId match {
        case source: SourceId => throw new IllegalArgumentException("SourceIds may not be executed.")
        case node: NodeId => {
          val dependencies = optimizedGraph.getDependencies(node)
          val depExpressions = dependencies.map(dep => execute(dep))
          val operator = optimizedGraph.getOperator(node)
          val expression = operator.execute(depExpressions)

          // Save state if the prefix is being considered for saving/loading state!
          // Note: This whole process isn't threadsafe & things could get messed up if
          // multiple pipelines are executing and/or optimizing at once
          if (prefixes.contains(node)) {
            PipelineEnv.getOrCreate.state(prefixes(node)) = expression
          }

          expression
        }
        case sink: SinkId => {
          val sinkDep = optimizedGraph.getSinkDependency(sink)
          execute(sinkDep)
        }
      }
    })
  }
}
