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
  def linearize(): Seq[NodeId]

  def getChildren(node: GraphId): Set[GraphId] = {
    // FIXME: Figure out if I should include Sinks as children!!! This current impl does not...
    node match {
      case id: NodeOrSourceId => {
        val instructionsWithIdAsDep = instructions.filter(_._2._2.contains(id))
        val nodeIds = instructionsWithIdAsDep.keySet
        nodeIds.map(x => x : GraphId)
      }
      case sinkId: SinkId => Set()
    }
  }

  def getDescendents(node: GraphId): Set[GraphId] = {
    val children = getChildren(node)
    children.map {
      child => getDescendents(child) + child
    }.fold(Set())(_ union _)
  }

  def getParents(node: GraphId): Set[NodeOrSourceId] = {
    // FIXME: Figure out if I should include Sources as parents!!! This current impl does...
    node match {
      case sourceId: SourceId => Set()
      case nodeId: NodeId => instructions(nodeId)._2.toSet
      case sinkId: SinkId => Set(sinks.toMap.apply(sinkId))
    }
  }

  def getAncestors(node: GraphId): Set[NodeOrSourceId] = {
    val parents = getParents(node)
    parents.map {
      parent => getAncestors(parent) + parent
    }.fold(Set())(_ union _)
  }

  def connectSinkToSource(sinkId: SinkId, sourceId: SourceId): InstructionGraph = {
    val sinkNode = sinks.toMap.get(sinkId).get
    require(sinkNode != sourceId, "Trying to connect a source into itself")
    val newInstructions = instructions.mapValues {
      case (node, deps) => (node, deps.map(dep => if (dep == sourceId) sinkNode else dep))
    }

    copy(instructions = newInstructions).removeSource(sourceId).removeSink(sinkId)
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

  // Need to add Utils to remove nodes, edges, sinks, sources
  def removeEdges(edges: Set[(NodeOrSourceId, NodeId)]): InstructionGraph = {
    val edgesToRemoveByNodeId = edges.groupBy(_._2).mapValues(_.map(_._1))
    val newInstructions = edgesToRemoveByNodeId.foldLeft(instructions) {
      case (curInstructions, (curNode, depsToRemove)) => {
        val curNodeInstruction = curInstructions(curNode)
        val newCurNodeInstruction = (
          curNodeInstruction._1,
          curNodeInstruction._2.filter(i => !depsToRemove.contains(i))
          )
        curInstructions.updated(curNode, newCurNodeInstruction)
      }
    }

    this.copy(instructions = newInstructions)
  }

  def removeEdge(a: NodeOrSourceId, b: NodeId): InstructionGraph = removeEdges(Set((a, b)))

  def removeSinks(sinksToRemove: Set[SinkId]): InstructionGraph = {
    val newSinks = sinks.filter(sink => !sinksToRemove.contains(sink._1))
    this.copy(sinks = newSinks)
  }
  def removeSink(sink: SinkId): InstructionGraph = removeSinks(Set(sink))

  // Throw an error if there are still edges connected to the sourceIds
  def removeSources(sourcesToRemove: Set[SourceId]): InstructionGraph
  def removeSource(source: SourceId): InstructionGraph = removeSources(Set(source))

  // when removing a node: turn all of its input deps into sinks, and anything that depends on it into a source
  // when removing multiple nodes: remove all edges in between them, then do the above (all ingress into sinks, all egress into sources)
  def removeNodes(nodes: Seq[NodeId]): (InstructionGraph, Seq[SourceId], Seq[SinkId])
  def removeNode(node: NodeId): (InstructionGraph, Seq[SourceId], Seq[SinkId]) = removeNodes(Seq(node))

  // Util to combine w/ another graph (potentially connecting/splicing some of the endpoints, but not necessarily)
  // Do I leave sinks that were spliced to? (I definitely don't leave sources that were spliced to)
  // For consistency, I won't leave sinks that were spliced to.
  def combine(
    otherGraph: InstructionGraph,
    otherSourceToThisSink: Map[SourceId, SinkId],
    thisSourceToOtherSink: Map[SourceId, SinkId]
  ): InstructionGraph

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