package workflow.graph

/**
 * To represent our Keystone workloads under the hood at the lowest level, we use a dataflow-esque DAG-like structure.
 * Data flows through vertices that manipulate it. At this level, everything is fully untyped! All type information
 * as it relates to ML-specific ideas is captured at higher API levels. More specifically, our workload graphs are
 * made up of three types of vertices, Nodes, Sources, and Sinks, with edges between them.
 *
 * Nodes consist of a unique id, and an Operator. Each Node also has zero, one, or multiple ordered dependencies
 * on Nodes and Sources elsewhere in the graph. These dependencies are represented as incoming edges in the DAG,
 * with data flowing into the node. Dependency order matters, as the incoming data is passed directly to the
 * Operator with each dependency as a separate parameter. Operators may take zero, one, or multiple ordered
 * input parameters, and always produce a single output.
 *
 * Next, we have Sources, a special type of vertex. A source is an incomplete input connection coming in from the
 * outside world. A workload cannot be executed without providing information about what data should be passed into
 * each Source. Sources are also treated by utility methods as endpoints of the DAG, useful for specifying how to
 * connect multiple workload DAGs together. Each Source has a unique id, and Sources always have zero
 * dependencies/incoming edges.
 *
 * Finally, we have Sinks, another special type of vertex. Sinks each have a unique id, and exactly one dependency
 * on either a source or a node. Sinks are output endpoints exposed to the outside world. They are used as markers
 * during execution to specify what results of executing a workload to output. Like Sources, they are also referenced
 * by utilities to to connect workloads together. Each Sink has a unique id, and exactly one dependency on either
 * a Node or a Source.
 *
 * @param sources  The set of all [[SourceId]]s of sources in the graph
 * @param sinkDependencies  A map of [[SinkId]] to the id of the node or source the sink depends on
 * @param operators  A map of [[NodeId]] to the operator contained within that node
 * @param dependencies  A map of [[NodeId]] to the node's ordered dependencies
 */
private[graph] case class Graph(
    sources: Set[SourceId],
    sinkDependencies: Map[SinkId, NodeOrSourceId],
    operators: Map[NodeId, Operator],
    dependencies: Map[NodeId, Seq[NodeOrSourceId]]
  ) {

  /**
   * The set of ids of all nodes in this graph.
   */
  val nodes: Set[NodeId] = operators.keySet

  /**
   * The set of ids of all sinks in this graph.
   */
  val sinks: Set[SinkId] = sinkDependencies.keySet

  /**
   * Get the dependencies of a given node in this graph.
   */
  def getDependencies(id: NodeId): Seq[NodeOrSourceId] = dependencies(id)

  /**
   * Get the dependency of a given sink in this graph.
   */
  def getSinkDependency(id: SinkId): NodeOrSourceId = sinkDependencies(id)

  /**
   * Get the operator at a given node in this graph.
   */
  def getOperator(id: NodeId): Operator = operators(id)

  /**
   * Get {@param num} [[NodeId]]s that don't clash with those of existing nodes.
   */
  private def nextNodeIds(num: Int): Seq[NodeId] = {
    val maxId = if (nodes.isEmpty) {
      0
    } else {
      nodes.map(_.id).max
    }

    (1 to num).map(i => NodeId(maxId + i))
  }

  /**
   * Get {@param num} [[SourceId]]s that don't clash with those of existing sources.
   */
  private def nextSourceIds(num: Int): Seq[SourceId] = {
    val maxId = (sources.map(_.id) + 0).max
    (1 to num).map(i => SourceId(maxId + i))
  }

  /**
   * Get {@param num} [[SinkId]]s that don't clash with those of existing sinks.
   */
  private def nextSinkIds(num: Int): Seq[SinkId] = {
    val maxId = (sinks.map(_.id) + 0).max
    (1 to num).map(i => SinkId(maxId + i))
  }

  /**
   * Get a [[NodeId]] that doesn't clash with any existing node in this graph.
   */
  private def nextNodeId(): NodeId = nextNodeIds(1).head

  /**
   * Get a [[SourceId]] that doesn't clash with any existing source in this graph.
   */
  private def nextSourceId(): SourceId = nextSourceIds(1).head

  /**
   * Get a [[SinkId]] that doesn't clash with any existing sink in this graph.
   */
  private def nextSinkId(): SinkId = nextSinkIds(1).head

  /**
   * Create a copy of this graph with an added node.
   *
   * @param op The operator to be stored at the new node
   * @param deps The dependencies of the new node
   * @return A pair containing the new graph and the id assigned to the new node
   */
  def addNode(op: Operator, deps: Seq[NodeOrSourceId]): (Graph, NodeId) = {
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      deps.forall(dep => nodesAndSources.contains(dep))
    }, "Node must have dependencies on existing ids")

    val id = nextNodeId()
    val newOperators = operators.updated(id, op)
    val newDependencies = dependencies.updated(id, deps)
    (copy(operators = newOperators, dependencies = newDependencies), id)
  }

  /**
   * Create a copy of this graph with an added sink.
   *
   * @param dep The dependency of the new sink
   * @return A pair containing the new graph and the id assigned to the new sink
   */
  def addSink(dep: NodeOrSourceId): (Graph, SinkId) = {
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      nodesAndSources.contains(dep)
    }, "Sink must have dependencies on an existing id")

    val id = nextSinkId()
    val newSinkDependencies = sinkDependencies.updated(id, dep)
    (copy(sinkDependencies = newSinkDependencies), id)
  }

  /**
   * Create a copy of this graph with an added source.
   *
   * @return A pair containing the new graph and the id assigned to the new source
   */
  def addSource(): (Graph, SourceId) = {
    val id = nextSourceId()
    val newSources = sources + id
    (copy(sources = newSources), id)
  }

  /**
   * Return a copy of this graph with the dependencies of a node updated.
   *
   * @param node The id of the node to be updated
   * @param deps The new dependencies to assign to the node
   * @return The new graph
   */
  def setDependencies(node: NodeId, deps: Seq[NodeOrSourceId]): Graph = {
    require(dependencies.contains(node), "Node being updated must exist")
    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      deps.forall(dep => nodesAndSources.contains(dep))
    }, "Node must have dependencies on existing ids")

    val newDependencies = dependencies.updated(node, deps)
    copy(dependencies = newDependencies)
  }

  /**
   * Return a copy of this graph with the operator of a node updated.
   *
   * @param node The id of the node to be updated
   * @param op The new operator to assign to the node
   * @return The new graph
   */
  def setOperator(node: NodeId, op: Operator): Graph = {
    require(dependencies.contains(node), "Node being updated must exist")

    val newOperators = operators.updated(node, op)
    copy(operators = newOperators)
  }

  /**
   * Return a copy of this graph with the dependency of a sink updated.
   *
   * @param sink The id of the sink to be updated
   * @param dep The new dependency to assign to the sink
   * @return The new graph
   */
  def setSinkDependency(sink: SinkId, dep: NodeOrSourceId): Graph = {
    require(sinkDependencies.contains(sink), "Sink being updated must exist")

    require ({
      val nodesAndSources: Set[NodeOrSourceId] = nodes ++ sources
      nodesAndSources.contains(dep)
    }, "Sink must have dependencies on an existing id")

    val newSinkDependencies = sinkDependencies.updated(sink, dep)
    copy(sinkDependencies = newSinkDependencies)
  }

  /**
   * Immutably remove a sink from this graph.
   *
   * @param sink The id of the sink to remove
   * @return The new graph
   */
  def removeSink(sink: SinkId): Graph = {
    require(sinkDependencies.contains(sink), "Sink being removed must exist")

    val newSinkDependencies = sinkDependencies - sink
    copy(sinkDependencies = newSinkDependencies)
  }

  /**
   * Immutably remove a source from this graph.
   * Note: This may leave invalid dangling dependencies on the deleted source,
   * that must be manually taken care of.
   *
   * @param source The id of the source to remove
   * @return The new graph
   */
  def removeSource(source: SourceId): Graph = {
    require(sources.contains(source), "Source being removed must exist")

    val newSources = sources - source
    copy(sources = newSources)
  }

  /**
   * Immutably remove a node from this graph.
   * Note: This may leave invalid dangling dependencies on the deleted node,
   * that must be manually taken care of.
   *
   * @param node The id of the node to remove
   * @return The new graph
   */
  def removeNode(node: NodeId): Graph = {
    require(nodes.contains(node), "Node being removed must exist")

    val newOperators = operators - node
    val newDependencies = dependencies - node
    copy(operators = newOperators, dependencies = newDependencies)
  }

  /**
   * Create a copy of this graph with all dependencies on a given node or source
   * replaced with a new target.
   *
   * @param oldDep The id of the old node or source that is the existing dependency target
   * @param newDep The id of the node or source intended to be the new target
   * @return The new graph
   */
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

    copy(sinkDependencies = newSinkDependencies, dependencies = newDependencies)
  }

  /**
   * Immutably attaches an entire other graph onto this one, without splicing together any Sinks or
   * Sources. The Sink, Source, and Node ids of the other graph may be reassigned to prevent
   * collisions with existing ids. So, this method makes sure to return mappings of old Sink and
   * Source ids in the graph being added to their newly assigned Sink and Source ids.
   *
   * @param graph  The graph to add into this graph
   * @return  A triple containing:
   *          - The new graph
   *          - A map of old id to new id for sources in the graph being added
   *          - A map of old id to new id for sinks in the graph being added
   */
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
    val newGraph = Graph(newSources, newSinkDependencies, newOperators, newDependencies)

    (newGraph, otherSourceIdMap, otherSinkIdMap)
  }

  /**
   * Immutably attaches an entire other graph onto this one, splicing Sinks of this graph to Sources of
   * the graph being added. The unspliced Sink, Source, and Node ids of the other graph may be reassigned
   * to prevent collisions with existing ids. So, this method makes sure to return mappings of old Sink and
   * Source ids in the graph being added to their newly assigned Sink and Source ids.
   *
   * All Sources and Sinks being spliced are removed from the newly created graph.
   *
   * @param graph  The graph to connect to this graph
   * @param spliceMap A map that specifies how to splice the Sources and Sinks.
   *                  Keys are Sources in the graph being added, and
   *                  Values are the Sinks in the existing graph to splice them to.
   * @return  A triple containing:
   *          - The new graph
   *          - A map of old id to new id for sources in the graph being added
   *          - A map of old id to new id for sinks in the graph being added
   */
  def connectGraph(
      graph: Graph,
      spliceMap: Map[SourceId, SinkId]
    ): (Graph, Map[SourceId, SourceId], Map[SinkId, SinkId]) = {
    require(spliceMap.keys.forall(source => graph.sources.contains(source)),
      "Must connect to sources that exist in the other graph")
    require(spliceMap.values.forall(sink => sinks.contains(sink)),
      "Must connect to sinks that exist in this graph")

    val (addedGraph, otherSourceIdMap, otherSinkIdMap) = addGraph(graph)

    val connectedGraph = spliceMap.foldLeft(addedGraph) {
      case (curGraph, (oldOtherSource, sink)) =>
        val source = otherSourceIdMap(oldOtherSource)
        val sinkDependency = getSinkDependency(sink)
        curGraph.replaceDependency(source, sinkDependency)
          .removeSource(source)
    }

    val newGraph = spliceMap.values.toSet.foldLeft(connectedGraph) {
      case (curGraph, sink) => curGraph.removeSink(sink)
    }

    (newGraph, otherSourceIdMap -- spliceMap.keySet, otherSinkIdMap)
  }

  /**
   * Create a copy of of this graph, with a set of nodes and their connections replaced by a different graph,
   * specifying how to connect it to the existing graph.
   *
   * @param nodesToRemove  The set of nodes to remove
   * @param replacement  The graph to insert in their place
   * @param replacementSourceSplice  A specification of how to connect the replacement graph to the existing graph.
   *                                 Key is a source of the replacement,
   *                                 Value is the node in the existing graph to connect the source to.
   * @param replacementSinkSplice  A specification of how to replace dangling dependencies on the nodes being removed.
   *                               Key is node being removed, Value is Sink of the replacement to depend on instead.
   * @return
   */
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
        val replacementDep = graph.getSinkDependency(sink)
        graph.replaceDependency(removedNode, replacementDep)
    }

    // Final validity check
    require ({
      val finalDeps = graphWithReplacementAndConnections.dependencies.values.flatMap(identity).toSet
      nodesToRemove.forall(removedNode => !finalDeps.contains(removedNode))
    }, "May not have any remaining dangling edges on the removed nodes")

    // Remove the sinks of the replacement
    replacementSinkIdMap.values.toSet.foldLeft(graphWithReplacementAndConnections) {
      case (graph, sink) => graph.removeSink(sink)
    }
  }

  /**
   * @return Generate a graphviz dot representation of this graph
   */
  def toDOTString: String = {
    val idToString: GraphId => String = {
      case SourceId(id) => s"Source_$id"
      case NodeId(id) => s"Node_$id"
      case SinkId(id) => s"Sink_$id"
    }

    val vertexLabels: Seq[String] = sources.toSeq.map(id => idToString(id) + " [label=\"" + id + "\" shape=\"Msquare\"]") ++
      nodes.toSeq.map(id => idToString(id) + s"[label=${'"' + getOperator(id).label + '"'}]") ++
      sinks.toSeq.map(id => idToString(id) + " [label=\"" + id + "\" shape=\"Msquare\"]")

    val dataEdges: Seq[String] = dependencies.toSeq.flatMap {
      case (id, deps) => deps.map(x => s"${idToString(x)} -> ${idToString(id)}")
    } ++ sinkDependencies.toSeq.map {
      case (id, dep) => s"${idToString(dep)} -> ${idToString(id)}"
    }

    val lines = vertexLabels ++ dataEdges
    lines.mkString("digraph pipeline {\n  rankdir=LR;\n  ", "\n  ", "\n}")
  }

}
