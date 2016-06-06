package workflow.graph

import org.apache.spark.rdd.RDD
import workflow.WorkflowUtils

/**
 * This class is used by the NodeOptimizationRule, to keep track
 * of the numPerPartitions and collect data samples at different parts of the graph
 *
 * Warning: Not thread-safe!
 *
 * @param graph The underlying graph to execute
 * @param samplesPerPartition The number of samples to take per partition
 */
private[graph] class SampleCollector(graph: Graph, samplesPerPartition: Int) {

  // The mutable internal state attached to the optimized graph. Lazily computed, requires optimization.
  private val executionState: scala.collection.mutable.Map[GraphId, Expression] =
    scala.collection.mutable.Map()

  val numPerPartition: scala.collection.mutable.Map[GraphId, Map[Int, Int]] = scala.collection.mutable.Map()

  // Internally used set of all ids in the optimized graph that are source ids, or depend explicitly or implicitly
  // on sources. Lazily computed, requires optimization.
  private val sourceDependants: Set[GraphId] = {
    graph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(graph, source) + source
    }
  }

  // Returns true if the node has a datum as a dependency
  def hasDatumDep(nodeId: NodeId): Boolean = {
    val deps = graph.getDependencies(nodeId).map(execute)
    deps.exists(_.isInstanceOf[DatumExpression])
  }

  // Get the sampled result of a node as an RDD, and the numPerPartition of what the unsampled RDD would have
  def getRDD[T](nodeId: NodeId): (RDD[T], Map[Int, Int]) = {
    val expression = execute(nodeId)
    require(expression.isInstanceOf[DatasetExpression], "Must be a Dataset Expression")

    val rdd = expression.get.asInstanceOf[RDD[T]]
    val npp = this.numPerPartition(nodeId)

    (rdd, npp)
  }

  /**
   * Execute the graph up to and including an input graph id, and return the result
   * of sampled execution at that id.
   * This method updates the internal execution state.
   *
   * @param graphId The GraphId to execute up to and including.
   * @return The sampled execution result at the input graph id.
   */
  def execute(graphId: GraphId): Expression = {
    require(!sourceDependants.contains(graphId), "May not execute GraphIds that depend on unconnected sources.")

    def handleDataset(node: NodeId, rdd: RDD[_]): DatasetOperator = {
      val npp = WorkflowUtils.numPerPartition(rdd)
      numPerPartition(node) = npp

      // Sample the RDD (with a value copy to avoid serializing this class when doing mapPartitions)
      val spp = samplesPerPartition
      val sampledRDD = rdd.mapPartitions(_.take(spp))
      DatasetOperator(sampledRDD)
    }

    executionState.getOrElseUpdate(graphId, {
      graphId match {
        case source: SourceId => throw new IllegalArgumentException("SourceIds may not be executed.")
        case node: NodeId => {
          val dependencies = graph.getDependencies(node)
          // Get the inputs, and make sure to cache any RDD inputs
          val depExpressions = dependencies.map(dep => execute(dep)).map {
            case exp: DatasetExpression => new DatasetExpression(exp.get.cache())
            case exp => exp
          }

          // Get the operator at the node, updating `numPerPartition` and sampling any RDDs as necessary.
          val operator = graph.getOperator(node) match {
            case op: ExpressionOperator => {
              op.execute(Seq()) match {
                // If the ExpressionOperator contains a dataset, sample the rdd and get the numPerPartition
                case exp: DatasetExpression => {
                  val rdd = exp.get
                  handleDataset(node, rdd)
                }

                case _ => {
                  numPerPartition(node) = Map(0 -> 1)
                  op
                }
              }
            }
            case DatasetOperator(rdd) => {
              handleDataset(node, rdd)
            }
            case op: DatumOperator => {
              numPerPartition(node) = Map(0 -> 1)
              op
            }
            case op: TransformerOperator => {
              numPerPartition(node) = numPerPartition(dependencies.head)
              op
            }
            case op: EstimatorOperator => {
              numPerPartition(node) = numPerPartition(dependencies.head)
              op
            }
            case op: DelegatingOperator => {
              // Get the numPerPartition from the first rdd input (the second dependency)
              numPerPartition(node) = numPerPartition(dependencies(1))
              op
            }
          }

          operator.execute(depExpressions)
        }
        case sink: SinkId => {
          val sinkDep = graph.getSinkDependency(sink)
          execute(sinkDep)
        }
      }
    })
  }

  /**
   * Unpersist any RDDs that this cached
   */
  def unpersist(): Unit = {
    executionState.values.foreach {
      case exp: DatasetExpression => exp.get.unpersist()
      case _ => Unit
    }
  }
}

/**
 * Node-level optimization, such as selecting a Linear Solver
 *
 * @param samplesPerPartition The number of items per partition to look at when optimizing nodes
 */
class NodeOptimizationRule(samplesPerPartition: Int = 3) extends Rule {

  def apply(graph: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    val descendantsOfSources = graph.sources.foldLeft(Set[GraphId]()) {
      case (descendants, source) => descendants ++ AnalysisUtils.getDescendants(graph, source) + source
    }

    val executor = new SampleCollector(graph, samplesPerPartition)

    // Get the set of nodes that we need to check for optimizations at.
    // This is any node that is Optimizable and does not depend on any source
    val nodesToOptimize = AnalysisUtils.linearize(graph).collect {
      case node: NodeId => (node, graph.getOperator(node))
    }.collect {
      case (node: NodeId, op: Operator with Optimizable) if !descendantsOfSources.contains(node) => (node, op)
    }

    // Find and insert the appropriate optimizations at each node to optimize.
    val newGraph = nodesToOptimize.foldLeft(graph) {
      case (curGraph, (node, op: OptimizableTransformer[a, b])) => {
        // Only optimize the transformer if it is operating on an RDD instead of a datum
        if (!executor.hasDatumDep(node)) {
          val dep = graph.getDependencies(node).head.asInstanceOf[NodeId]
          val (depRDD, numPerPartition) = executor.getRDD[a](dep)

          val optimizedOp = op.optimize(depRDD, numPerPartition)

          curGraph.setOperator(node, optimizedOp)
        } else {
          curGraph
        }
      }
      case (curGraph, (node, op: OptimizableEstimator[a, b])) => {
        val dep = graph.getDependencies(node).head.asInstanceOf[NodeId]
        val (depRDD, numPerPartition) = executor.getRDD[a](dep)

        val optimizedOp = op.optimize(depRDD, numPerPartition)

        curGraph.setOperator(node, optimizedOp)
      }
      case (curGraph, (node, op: OptimizableLabelEstimator[a, b, l])) => {
        val (dataDep, numPerPartition) = executor.getRDD[a](graph.getDependencies(node).head.asInstanceOf[NodeId])
        val labelsDep = executor.getRDD[l](graph.getDependencies(node).apply(1).asInstanceOf[NodeId])._1

        val optimizedOp = op.optimize(dataDep, labelsDep, numPerPartition)

        curGraph.setOperator(node, optimizedOp)
      }
    }

    // Unpersist anything the executor cached
    executor.unpersist()

    (newGraph, prefixes)
  }
}
