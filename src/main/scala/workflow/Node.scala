package workflow

import org.apache.spark.rdd.RDD

sealed trait Instruction {
  def getDependencies: Seq[Int]
  def mapDependencies(func: Int => Int): Instruction
  def execute(deps: Seq[InstructionOutput]): InstructionOutput
}

sealed trait InstructionOutput
abstract class DataOutput extends InstructionOutput
case class RDDOutput(rdd: RDD[_]) extends DataOutput
case class SingleDataOutput(data: Any) extends DataOutput
case class EstimatorOutput(estimatorNode: EstimatorNode) extends InstructionOutput
case class TransformerOutput(transformerNode: TransformerNode) extends InstructionOutput

/**
 * TransformerApplyNode is an Instruction that represents the
 * application of a transformer to data inputs.
 *
 * @param transformer The index of the [[TransformerNode]] in the instructions
 * @param inputs The indices of the data inputs in the instructions
 */
private[workflow] case class TransformerApplyNode(transformer: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(transformer) ++ inputs
  override def mapDependencies(func: (Int) => Int): TransformerApplyNode = {
    TransformerApplyNode(func(transformer), inputs.map(func))
  }

  override def execute(deps: Seq[InstructionOutput]): DataOutput = {
    val transformer = deps.collectFirst {
      case TransformerOutput(t) => t
    }.get

    val dataInputs = deps.tail.collect {
      case RDDOutput(rdd) => rdd
    }

    RDDOutput(transformer.transformRDD(dataInputs.toIterator))
  }
}

/**
 * An Instruction that represents the fitting of an estimator with data inputs.
 *
 * @param est The index of the [[EstimatorNode]] in the instructions
 * @param inputs The indices of the data inputs in the instructions
 */
private[workflow] case class EstimatorFitNode(est: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(est) ++ inputs
  override def mapDependencies(func: (Int) => Int): EstimatorFitNode = {
    EstimatorFitNode(func(est), inputs.map(func))
  }

  override def execute(deps: Seq[InstructionOutput]): TransformerOutput = {
    val estimator = deps.collectFirst {
      case EstimatorOutput(e) => e
    }.get

    val dataInputs = deps.tail.collect {
      case RDDOutput(rdd) => rdd
    }

    TransformerOutput(estimator.fitRDDs(dataInputs.toIterator))
  }
}

sealed trait GraphId { val id: Long }
sealed trait NodeOrSourceId extends GraphId
case class NodeId(id: Long) extends NodeOrSourceId
case class SourceId(id: Long) extends NodeOrSourceId
case class SinkId(id: Long) extends GraphId

trait Operator
class Graph(
    val sources: Set[SourceId],
    val sinkDependencies: Map[SinkId, NodeOrSourceId],
    val operators: Map[NodeId, Operator],
    val dependencies: Map[NodeId, Seq[NodeOrSourceId]]
  ) {
  def nodes: Set[NodeId] = operators.keySet
  def sinks: Set[SinkId] = sinkDependencies.keySet

  def getDependencies(id: NodeId): Seq[NodeOrSourceId] = dependencies(id)
  def getSinkDependency(id: SinkId): NodeOrSourceId = sinkDependencies(id)
  def getOperator(id: NodeId): Operator = operators(id)

  private def nextNodeIds(num: Int): Seq[NodeId] = {
    val maxId = (nodes.map(_.id) + 0).max
    (1 to num).map(i => NodeId(maxId + i))
  }

  private def nextSourceIds(num: Int): Seq[SourceId] = {
    val maxId = (sources.map(_.id) + 0).max
    (1 to num).map(i => SourceId(maxId + i))
  }

  private def nextSinkIds(num: Int): Seq[SinkId] = {
    val maxId = (sinks.map(_.id) + 0).max
    (1 to num).map(i => SinkId(maxId + i))
  }

  private def nextNodeId(): NodeId = nextNodeIds(1).head
  private def nextSourceId(): SourceId = nextSourceIds(1).head
  private def nextSinkId(): SinkId = nextSinkIds(1).head

  def addNode(op: Operator, deps: Seq[NodeOrSourceId]): (Graph, NodeId) = {
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      deps.forall(dep => nodesAndSources.contains(dep))
    }, "Node must have dependencies on existing ids")

    val id = nextNodeId()
    val newOperators = operators.updated(id, op)
    val newDependencies = dependencies.updated(id, deps)
    (new Graph(sources, sinkDependencies, newOperators, newDependencies), id)
  }

  def addSink(dep: NodeOrSourceId): (Graph, SinkId) = {
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      nodesAndSources.contains(dep)
    }, "Sink must have dependencies on an existing id")

    val id = nextSinkId()
    val newSinkDependencies = sinkDependencies.updated(id, dep)
    (new Graph(sources, newSinkDependencies, operators, dependencies), id)
  }

  def addSource(): (Graph, SourceId) = {
    val id = nextSourceId()
    val newSources = sources + id
    (new Graph(newSources, sinkDependencies, operators, dependencies), id)
  }

  def setDependencies(node: NodeId, deps: Seq[NodeOrSourceId]): Graph = {
    require(dependencies.contains(node), "Node being updated must exist")
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      deps.forall(dep => nodesAndSources.contains(dep))
    }, "Node must have dependencies on existing ids")

    val newDependencies = dependencies.updated(node, deps)
    new Graph(sources, sinkDependencies, operators, newDependencies)
  }

  def setOperator(node: NodeId, op: Operator): Graph = {
    require(dependencies.contains(node), "Node being updated must exist")

    val newOperators = operators.updated(node, op)
    new Graph(sources, sinkDependencies, newOperators, dependencies)
  }

  def setSinkDependency(sink: SinkId, dep: NodeOrSourceId): Graph = {
    require(sinkDependencies.contains(sink), "Sink being updated must exist")

    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      nodesAndSources.contains(dep)
    }, "Sink must have dependencies on an existing id")

    val newSinkDependencies = sinkDependencies.updated(sink, dep)
    new Graph(sources, newSinkDependencies, operators, dependencies)
  }

  def removeSink(sink: SinkId): Graph = {
    require(sinkDependencies.contains(sink), "Sink being removed must exist")

    val newSinkDependencies = sinkDependencies - sink
    new Graph(sources, newSinkDependencies, operators, dependencies)
  }

  def removeSource(source: SourceId): Graph = {
    require(sources.contains(source), "Source being removed must exist")

    val newSources = sources - source
    new Graph(newSources, sinkDependencies, operators, dependencies)
  }

  def removeNode(node: NodeId): Graph = {
    require(nodes.contains(node), "Node being removed must exist")

    val newOperators = operators - node
    val newDependencies = dependencies - node
    new Graph(sources, sinkDependencies, newOperators, newDependencies)
  }

  def replaceDependency(oldDep: NodeOrSourceId, newDep: NodeOrSourceId): Graph = {
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      nodesAndSources.contains(newDep)
    }, "Replacement dependency id must exist")

    val newDependencies = dependencies.map { nodeAndDeps =>
      val newNodeDeps = nodeAndDeps._2.map(dep => if (dep == oldDep) newDep else dep)
      (nodeAndDeps._1, newNodeDeps)
    }

    val newSinkDependencies = sinkDependencies.map { sinkAndDep =>
      val newSinkDep = if (sinkAndDep._2 == oldDep) newDep else sinkAndDep._2
      (sinkAndDep._1, newSinkDep)
    }

    new Graph(sources, newSinkDependencies, operators, newDependencies)
  }

  def addGraph(graph: Graph): (Graph, Map[SourceId, SourceId], Map[SinkId, SinkId]) = {
    // Generate the new ids and mappings of old to new ids
    val newSourceIds = nextSourceIds(graph.sources.size)
    val newNodeIds = nextNodeIds(graph.nodes.size)
    val newSinkIds = nextSinkIds(graph.sinks.size)

    val otherSources = graph.sources.toSeq
    val otherNodes = graph.nodes.toSeq
    val otherSinks = graph.sinks.toSeq

    val otherSourceIdMap: Map[SourceId, SourceId] = otherSources.zip(newSourceIds).toMap
    val otherNodeIdMap: Map[NodeId, NodeId] = otherNodes.zip(newNodeIds).toMap
    val otherSinkIdMap: Map[SinkId, SinkId] = otherSinks.zip(newSinkIds).toMap

    val otherNodeOrSourceIdMap: Map[NodeOrSourceId, NodeOrSourceId] = otherSourceIdMap ++ otherNodeIdMap

    // Build the new combined graph
    val newOperators = operators ++ graph.operators.map {
      case (nodeId, op) => (otherNodeIdMap(nodeId), op)
    }
    val newDependencies = dependencies ++ graph.dependencies.map {
      case (nodeId, deps) => (otherNodeIdMap(nodeId), deps.map(otherNodeOrSourceIdMap))
    }
    val newSources = sources ++ newSourceIds
    val newSinkDependencies = sinkDependencies ++ graph.sinkDependencies.map {
      case (sinkId, dep) => (otherSinkIdMap(sinkId), otherNodeOrSourceIdMap(dep))
    }
    val newGraph = new Graph(newSources, newSinkDependencies, newOperators, newDependencies)

    (newGraph, otherSourceIdMap, otherSinkIdMap)
  }

  def connectGraph(graph: Graph, spliceMap: Map[SourceId, SinkId]): Graph = {
    require(spliceMap.keys.forall(source => graph.sources.contains(source)),
      "Must connect to sources that exist in the other graph")
    require(spliceMap.values.forall(sink => sinks.contains(sink)),
      "Must connect to sinks that exist in this graph")

    val (addedGraph, otherSourceIdMap, _) = addGraph(graph)

    val connectedGraph = spliceMap.foldLeft(addedGraph) {
      case (curGraph, (oldOtherSource, sink)) =>
        val source = otherSourceIdMap(oldOtherSource)
        val sinkDependency = getSinkDependency(sink)
        curGraph.replaceDependency(source, sinkDependency)
          .removeSource(source)
    }

    spliceMap.values.toSet.foldLeft(connectedGraph) {
      case (curGraph, sink) => curGraph.removeSink(sink)
    }
  }

  def replaceNodes(
      nodesToRemove: Set[NodeId],
      replacement: Graph,
      replacementSourceSplice: Map[SourceId, NodeOrSourceId],
      replacementSinkSplice: Map[NodeId, SinkId]
    ): Graph = {
    // Illegal argument checks
    require(replacementSinkSplice.values.toSet == replacement.sinks, "Must attach all of the replacement's sinks")
    require(replacementSinkSplice.keys.forall(nodesToRemove), "May only replace dependencies on removed nodes")
    require(replacementSourceSplice.keySet == replacement.sources, "Must attach all of the replacement's sources")
    require(replacementSourceSplice.values.forall {
      case id: NodeId => !nodesToRemove.contains(id)
      case _ => true
    }, "May not connect replacement sources to nodes being removed")
    require(replacementSourceSplice.values.forall {
      case id: NodeId => nodes.contains(id)
      case id: SourceId => sources.contains(id)
    }, "May only connect replacement sources to existing nodes")

    // Remove the nodes
    val withNodesRemoved = nodesToRemove.foldLeft(this) {
      case (graph, node) => graph.removeNode(node)
    }

    // Add the replacement, without connecting sinks or sources yet
    val (graphWithReplacement, replacementSourceIdMap, replacementSinkIdMap) = withNodesRemoved.addGraph(replacement)

    // Connect the sources of the replacement to the existing graph
    val graphWithReplacementAndSourceConnections = replacementSourceSplice.foldLeft(graphWithReplacement) {
      case (graph, (oldSource, node)) =>
        val source = replacementSourceIdMap(oldSource)
        graph.replaceDependency(source, node).removeSource(source)
    }

    // Connect the sinks from the replacement to dangling dependencies on now-removed nodes
    val graphWithReplacementAndConnections = replacementSinkSplice.foldLeft(graphWithReplacementAndSourceConnections) {
      case (graph, (removedNode, oldSink)) =>
        val sink = replacementSinkIdMap(oldSink)
        graph.replaceDependency(removedNode, getSinkDependency(sink))
    }

    // Final validity check
    require ({
      val finalDeps = graphWithReplacementAndConnections.dependencies.values.flatMap(identity).toSet
      nodesToRemove.forall(removedNode => !finalDeps.contains(removedNode))
    }, "May not have any remaining dangling edges on the removed nodes")

    // Remove the sinks of the replacement
    replacementSinkSplice.values.toSet.foldLeft(graphWithReplacementAndConnections) {
      case (graph, sink) => graph.removeSink(sink)
    }
  }

  def linearize(): Seq[GraphId] = {
    def linearize(graphId: GraphId): Seq[GraphId] = {
      val deps: Seq[GraphId] = graphId match {
        case source: SourceId => Seq()
        case node: NodeId => getDependencies(node)
        case sink: SinkId => Seq(getSinkDependency(sink))
      }
      deps.foldLeft(Seq[GraphId]()) {
        case (linearization, dep) => if (!linearization.contains(dep)) {
          linearization ++ linearize(dep).filter(id => !linearization.contains(id))
        } else {
          linearization
        }
      } :+ graphId
    }

    sinks.foldLeft(Seq[GraphId]()) {
      case (linearization, sink) => {
        linearization ++ linearize(sink).filter(id => !linearization.contains(id))
      }
    }
  }

  def getChildren(node: GraphId): Set[GraphId] = {
    node match {
      case id: NodeOrSourceId => {
        val childrenNodes = dependencies.filter(_._2.contains(id)).keySet
        val childrenSinks = sinkDependencies.filter(_._2 == id).keySet
        childrenNodes ++ childrenSinks
      }
      case sinkId: SinkId => Set()
    }
  }

  def getDescendants(node: GraphId): Set[GraphId] = {
    val children = getChildren(node)
    children.map {
      child => getDescendants(child) + child
    }.fold(Set())(_ union _)
  }

  def getParents(node: GraphId): Set[NodeOrSourceId] = {
    node match {
      case source: SourceId => Set()
      case node: NodeId => getDependencies(node).toSet
      case sink: SinkId => Set(getSinkDependency(sink))
    }
  }

  def getAncestors(node: GraphId): Set[NodeOrSourceId] = {
    val parents = getParents(node)
    parents.map {
      parent => getAncestors(parent) + parent
    }.fold(Set())(_ union _)
  }

}

sealed trait Node  {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }
}

private[workflow] abstract class EstimatorNode extends Node with Serializable with Instruction {
  private[workflow] def fitRDDs(dependencies: Iterator[RDD[_]]): TransformerNode
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): EstimatorNode = this
  override def execute(deps: Seq[InstructionOutput]): EstimatorOutput = EstimatorOutput(this)
}

private[workflow] abstract class TransformerNode extends Node with Serializable with Instruction {
  private[workflow] def transform(dataDependencies: Iterator[_]): Any
  private[workflow] def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[_]

  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): TransformerNode = this
  override def execute(deps: Seq[InstructionOutput]): TransformerOutput = TransformerOutput(this)
}

private[workflow] case class SourceNode(rdd: RDD[_]) extends Node with Instruction {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): SourceNode = this
  override def execute(deps: Seq[InstructionOutput]): RDDOutput = {
    RDDOutput(rdd)
  }
}

/**
 * A node used internally to apply the fit output of an Estimator onto data dependencies.
 * Takes one fit dependency, and directly applies its transform onto the data dependencies.
 *
 * Only expects one fit dependency. This is because the DSL will place a new DelegatingTransformer
 * after each Estimator whenever chaining an Estimator, and connect it via a fit dependency.
 */
private[workflow] class DelegatingTransformerNode(override val label: String)
  extends Node with Serializable