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

sealed trait GraphId {
  val id: Long
}

sealed trait NodeOrSourceId extends GraphId
case class NodeId(id: Long) extends NodeOrSourceId
case class SourceId(id: Long) extends NodeOrSourceId
case class SinkId(id: Long) extends GraphId

// Currently this only contains DAG manipulation utils
case class InstructionGraph(
    instructions: Map[NodeId, (Node, Seq[NodeOrSourceId])],
    sources: Seq[SourceId],
    sinks: Seq[(SinkId, NodeOrSourceId)]
  ) {

  // Analysis utils
  def maxId: Long = {
    val allIds = instructions.keys.map(_.id) ++ sources.map(_.id) ++ sinks.map(_._1.id)
    allIds.max
  }

  // Other Analysis Utils:
  def linearize(): Seq[GraphId] = {
    def linearize(graphId: GraphId): Seq[GraphId] = {
      getOrderedParents(graphId).foldLeft(Seq[GraphId]()) {
        case (linearization, parent) => if (!linearization.contains(parent)) {
          linearization ++ linearize(parent).filter(id => !linearization.contains(id))
        } else {
          linearization
        }
      } :+ graphId
    }

    sinks.map(_._1).foldLeft(Seq[GraphId]()) {
      case (linearization, sink) => {
        linearization ++ linearize(sink).filter(id => !linearization.contains(id))
      }
    }
  }

  def getOrderedChildren(node: GraphId): Seq[GraphId] = {
    // FIXME: Figure out if I should include Sinks as children!!! This current impl does...
    node match {
      case id: NodeOrSourceId => {
        val instructionsWithIdAsDep = instructions.filter(_._2._2.contains(id))
        val childrenNodes = instructionsWithIdAsDep.keys.toSeq
        val childrenSinks = sinks.filter(_._2 == id).map(_._1)
        childrenNodes ++ childrenSinks
      }
      case sinkId: SinkId => Seq()
    }
  }

  def getChildren(node: GraphId): Set[GraphId] = {
    getOrderedChildren(node).toSet
  }

  def getDescendents(node: GraphId): Set[GraphId] = {
    val children = getChildren(node)
    children.map {
      child => getDescendents(child) + child
    }.fold(Set())(_ union _)
  }

  def getOrderedParents(node: GraphId): Seq[NodeOrSourceId] = {
    // FIXME: Figure out if I should include Sources as parents!!! This current impl does...
    node match {
      case sourceId: SourceId => Seq()
      case nodeId: NodeId => instructions(nodeId)._2
      case sinkId: SinkId => Seq(sinks.toMap.apply(sinkId))
    }
  }

  def getParents(node: GraphId): Set[NodeOrSourceId] = {
    getOrderedParents(node).toSet
  }

  def getAncestors(node: GraphId): Set[NodeOrSourceId] = {
    val parents = getParents(node)
    parents.map {
      parent => getAncestors(parent) + parent
    }.fold(Set())(_ union _)
  }

  def connectSinkToSource(sinkId: SinkId, sourceId: SourceId): InstructionGraph = {
    val sinkNode = sinks.toMap.get(sinkId).get
    require(sinkNode != sourceId, "Cannot connect a source into itself")
    val newInstructions = instructions.mapValues {
      case (node, deps) => (node, deps.map(dep => if (dep == sourceId) sinkNode else dep))
    }

    copy(instructions = newInstructions).removeSource(sourceId).removeSink(sinkId)
  }

  def connectSinkToNode(sinkId: SinkId, nodeId: NodeId): InstructionGraph = {
    val sinkNode = sinks.toMap.get(sinkId).get
    require(sinkNode != nodeId, "Cannot connect a node into itself")
    val curInstruction = instructions(nodeId)
    val newInstruction = (curInstruction._1, curInstruction._2 :+ sinkNode)
    val newInstructions = instructions.updated(nodeId, newInstruction)

    copy(instructions = newInstructions).removeSink(sinkId)
  }

  def connectNodeToSource(nodeId: NodeOrSourceId, sourceId: SourceId): InstructionGraph = {
    val newInstructions = instructions.mapValues {
      case (node, deps) => (node, deps.map(dep => if (dep == sourceId) nodeId else dep))
    }

    copy(instructions = newInstructions).removeSource(sourceId)
  }

  def addEdge(a: NodeOrSourceId, b: NodeId): InstructionGraph = addEdges(Seq((a, b)))
  def addEdges(edges: Seq[(NodeOrSourceId, NodeId)]): InstructionGraph = {
    val depEdgesToAdd = edges.groupBy(_._2).mapValues(_.map(_._1))
    val newInstructions = depEdgesToAdd.foldLeft(instructions) {
      case (curInstructions, (nodeId, deps)) =>
        val curInstruction = curInstructions(nodeId)
        val newInstruction = (curInstruction._1, curInstruction._2 ++ deps)
        curInstructions.updated(nodeId, newInstruction)
    }

    this.copy(instructions = newInstructions)
  }

  def addSinks(nodes: Seq[NodeOrSourceId]): (InstructionGraph, Seq[SinkId]) = {
    require(nodes.forall {
      case sourceId: SourceId => sources.contains(sourceId)
      case nodeId: NodeId => instructions.contains(nodeId)
    }, "All node ids being assigned to a sink must be nodes or sources in the graph.")

    val newIdStart = maxId + 1
    val newSinks = nodes.zipWithIndex.map {
      case (nodeId, index) =>
        (SinkId(newIdStart + index), nodeId)
    }
    val newGraph = this.copy(sinks = this.sinks ++ newSinks)

    (newGraph, newSinks.map(_._1))
  }

  def addSink(node: NodeOrSourceId): (InstructionGraph, SinkId) = {
    val (graph, newSinks) = addSinks(Seq(node))
    (graph, newSinks.head)
  }

  def addSources(numSources: Int): (InstructionGraph, Seq[SourceId]) = {
    val newIdStart = maxId + 1
    val newSources = (0 until numSources).map(i => SourceId(newIdStart + i))
    val newGraph = this.copy(sources = this.sources ++ newSources)
    (newGraph, newSources)
  }

  def addSource(): (InstructionGraph, SourceId) = {
    val (graph, newSources) = addSources(1)
    (graph, newSources.head)
  }

  def addNodes(newNodes: Seq[Node]): (InstructionGraph, Seq[NodeId]) = {
    val newIdStart = maxId + 1
    val newNodesWithIds = newNodes.zipWithIndex.map {
      case (node, index) =>
        (NodeId(newIdStart + index), (node, Seq[NodeOrSourceId]()))
    }

    val newGraph = this.copy(instructions = this.instructions ++ newNodesWithIds)
    (newGraph, newNodesWithIds.map(_._1))
  }

  def addNode(node: Node): (InstructionGraph, NodeId) = {
    val (graph, newNodes) = addNodes(Seq(node))
    (graph, newNodes.head)
  }

  def removeSinks(sinksToRemove: Set[SinkId]): InstructionGraph = {
    val newSinks = sinks.filter(sink => !sinksToRemove.contains(sink._1))
    this.copy(sinks = newSinks)
  }
  def removeSink(sink: SinkId): InstructionGraph = removeSinks(Set(sink))

  // Throw an error if there are still edges connected to the sourceIds
  def removeSources(sourcesToRemove: Set[SourceId]): InstructionGraph = {
    sourcesToRemove.foreach {
      source => require(getChildren(source).isEmpty, "Cannot remove sources that are still depended on")
    }

    val newSources = sources.filterNot(sourcesToRemove)
    copy(sources = newSources)
  }

  def removeSource(source: SourceId): InstructionGraph = removeSources(Set(source))

  def replaceNodes(
    nodesToRemove: Set[NodeId],
    replacement: InstructionGraph,
    graphSourceConnections: Map[SourceId, NodeOrSourceId],
    removedNodeToReplacementSinks: Map[NodeId, SinkId]
  ): InstructionGraph = {
    // requirements:
    // - all nodes being removed w/ external edges on them are being replaced w/ a sink of the replacement
    // - All Sources of the replacement are being connected to a NodeOrSourceId that exists in the initial graph
    // - All sinks in the replacement end up connected to an edge previously on a node being removed

    // Order:
    // - (done) Remove nodes from Instruction keys
    // - (done) do requires
    // - (done) addGraph replacement, making sure to track old->new sink and source mappings
    // - (done) connectNodeToSource
    // - not exactly a connectSinkToNode, more of a replaceEdge...
    // - removeSinks on the newly connected sinks
    val graphWithNodesRemoved = this.copy(instructions = instructions.filterKeys(id => !nodesToRemove.contains(id)))
    require(graphSourceConnections.keys.toSet == replacement.sources.toSet)
    require(removedNodeToReplacementSinks.values.toSet == replacement.sinks.map(_._1).toSet)
    require(removedNodeToReplacementSinks.keys.forall(id => nodesToRemove.contains(id)))
    require(graphSourceConnections.values.forall {
      case id: NodeId => !nodesToRemove.contains(id)
      case _ => true
    })
    require(nodesToRemove.forall { id =>
      if (graphWithNodesRemoved.getChildren(id).nonEmpty) {
        removedNodeToReplacementSinks.contains(id)
      } else {
        true
      }
    })

    val (graphWithReplacement, replacementSourceIdMap, replacementSinkIdMap) =
      graphWithNodesRemoved.addGraph(replacement)

    val graphWithReplacementAndSourceConnections = graphSourceConnections.foldLeft(graphWithReplacement) {
      case (curGraph, (replacementSource, nodeToConnectToSource)) =>
        curGraph.connectNodeToSource(nodeToConnectToSource, replacementSourceIdMap(replacementSource))
    }

    val graphWithReplacementAndConnections = removedNodeToReplacementSinks.foldLeft(graphWithReplacementAndSourceConnections) {
      case (curGraph, (removedNode, sinkToUse)) =>
        val idToInsert = curGraph.sinks.toMap.apply(replacementSinkIdMap(sinkToUse))
        val newInstructions = curGraph.instructions.map {
          case (id, (node, deps)) => (id, (node, deps.map(dep => if (dep == removedNode) idToInsert else dep)))
        }

        curGraph.copy(instructions = newInstructions)
    }

    graphWithReplacementAndConnections.removeSinks(replacementSinkIdMap.values.toSet)
  }

  def addGraph(otherGraph: InstructionGraph): (InstructionGraph, Map[SourceId, SourceId], Map[SinkId, SinkId]) = {
    val newIdStart = maxId + 1
    val otherSourceIds = otherGraph.sources
    val otherNodeIds = otherGraph.instructions.keys
    val otherSinkIds = otherGraph.sinks.map(_._1)

    val otherSourceIdMap: Map[SourceId, SourceId] = otherSourceIds.zipWithIndex.toMap.mapValues(i => SourceId(i + newIdStart))
    val otherNodeIdMap: Map[NodeId, NodeId] = otherNodeIds.zipWithIndex.toMap.mapValues(i => NodeId(i + newIdStart + otherSourceIdMap.size))
    val otherNodeOrSourceIdMap: Map[NodeOrSourceId, NodeOrSourceId] = otherSourceIdMap ++ otherNodeIdMap
    val otherSinkIdMap: Map[SinkId, SinkId] = otherSinkIds.zipWithIndex.toMap.mapValues(i => SinkId(i + newIdStart + otherNodeOrSourceIdMap.size))

    val newInstructions = instructions ++ otherGraph.instructions.map {
      case (nodeId, (node, deps)) => (otherNodeIdMap(nodeId), (node, deps.map(otherNodeOrSourceIdMap)))
    }
    val newSources = sources ++ otherGraph.sources.map(otherSourceIdMap)
    val newSinks = sinks ++ otherGraph.sinks.map {
      case (sinkId, nodeId) => (otherSinkIdMap(sinkId), otherNodeOrSourceIdMap(nodeId))
    }

    val newGraph = new InstructionGraph(newInstructions, newSources, newSinks)
    (newGraph, otherSourceIdMap, otherSinkIdMap)
  }

  // Util to combine w/ another graph (potentially connecting/splicing some of the endpoints, but not necessarily)
  // Do I leave sinks that were spliced to? (I definitely don't leave sources that were spliced to)
  // For consistency, I won't leave sinks that were spliced to.
  def combine(
    otherGraph: InstructionGraph,
    otherSinkToThisSource: Seq[(SinkId, SourceId)],
    thisSinkToOtherSource: Seq[(SinkId, SourceId)]
  ): InstructionGraph = {
    require(otherSinkToThisSource.map(_._1).toSet.size == otherSinkToThisSource.size,
      "May only connect each sink/source once!")
    require(otherSinkToThisSource.map(_._2).toSet.size == otherSinkToThisSource.size,
      "May only connect each sink/source once!")
    require(thisSinkToOtherSource.map(_._1).toSet.size == otherSinkToThisSource.size,
      "May only connect each sink/source once!")
    require(thisSinkToOtherSource.map(_._2).toSet.size == otherSinkToThisSource.size,
      "May only connect each sink/source once!")
    val (addedGraph, otherSourceIdMap, otherSinkIdMap) = addGraph(otherGraph)

    val graphWithSomeConnections = otherSinkToThisSource.foldLeft(addedGraph) {
      case (curGraph, (sinkId, sourceId)) => curGraph.connectSinkToSource(otherSinkIdMap(sinkId), sourceId)
    }

    val newGraph = thisSinkToOtherSource.foldLeft(graphWithSomeConnections) {
      case (curGraph, (sinkId, sourceId)) => curGraph.connectSinkToSource(sinkId, otherSourceIdMap(sourceId))
    }

    newGraph
  }

  // Maybe also a util to re-assign all Node, Source, & Sink ids to not collide w/ a given set?
    // - map each GraphId to a unique index
    // - get a "non-collision" index
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