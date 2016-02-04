package workflow

import org.apache.spark.rdd.RDD

object WorkflowUtils {
  def instructionsToPipeline[A, B](instructions: Seq[Instruction]): Pipeline[A, B] = {
    val (newNodes, newDataDeps, newFitDeps, _) = instructions.indices.foldLeft(
      (Seq[Node](), Seq[Seq[Int]](), Seq[Option[Int]](), Map(Pipeline.SOURCE -> Pipeline.SOURCE))
    ) {
      case ((nodes, dataDeps, fitDeps, idMap), instruction) =>
        instructions(instruction) match {
          case est: EstimatorNode => (nodes, dataDeps, fitDeps, idMap)
          case transformer: TransformerNode => (nodes, dataDeps, fitDeps, idMap)
          case source: SourceNode =>
            (nodes :+ source, dataDeps :+ Seq(), fitDeps :+ None, idMap + (instruction -> nodes.length))
          case TransformerApplyNode(transformer, inputs) => {
            instructions(transformer) match {
              case transformerNode: TransformerNode => (
                nodes :+ transformerNode,
                dataDeps :+ inputs.map(idMap.apply),
                fitDeps :+ None,
                idMap + (instruction -> nodes.length))

              case EstimatorFitNode(est, estInputs) => (
                nodes :+ new DelegatingTransformerNode(
                  s"Fit[${instructions(est).asInstanceOf[EstimatorNode].label}]"),
                dataDeps :+ inputs.map(idMap.apply),
                fitDeps :+ Some(idMap(transformer)),
                idMap + (instruction -> nodes.length))

              case _ => throw new RuntimeException("Transformer apply instruction must point at a Transformer")
            }
          }
          case EstimatorFitNode(est, inputs) => (
            nodes :+ instructions(est).asInstanceOf[EstimatorNode],
            dataDeps :+ inputs.map(idMap.apply),
            fitDeps :+ None,
            idMap + (instruction -> nodes.length))
        }
    }

    new ConcretePipeline(newNodes, newDataDeps, newFitDeps, newNodes.length - 1)
  }

  /**
   * Linearizes a pipeline DAG into a Seq[Instruction]
   * by walking backwards from the sink in a depth-first manner
   */
  def pipelineToInstructions[A, B](pipeline: Pipeline[A, B]): Seq[Instruction] = {
    val nodes = pipeline.nodes
    val dataDeps = pipeline.dataDeps
    val fitDeps = pipeline.fitDeps
    val sink = pipeline.sink

    pipelineToInstructionsRecursion(sink, nodes, dataDeps, fitDeps, Map(Pipeline.SOURCE -> Pipeline.SOURCE), Seq())._2
  }

  def pipelineToInstructionsRecursion(
      current: Int,
      nodes: Seq[Node],
      dataDeps: Seq[Seq[Int]],
      fitDeps: Seq[Option[Int]],
      nodeIdToInstructionId: Map[Int, Int],
      instructions: Seq[Instruction]
    ): (Map[Int, Int], Seq[Instruction]) = {

    val (newIdMap, newInstructions) = (fitDeps(current) ++ dataDeps(current))
      .foldLeft((nodeIdToInstructionId, instructions)) {
      case ((curIdMap, curInstructions), dep)
        if !curIdMap.contains(dep) && dep != Pipeline.SOURCE =>
        pipelineToInstructionsRecursion(dep, nodes, dataDeps, fitDeps, curIdMap, curInstructions)
      case ((curIdMap, curInstructions), _) => (curIdMap, curInstructions)
    }

    val dataInputs = dataDeps(current).map(newIdMap.apply)

    nodes(current) match {
      case source: SourceNode =>
        (newIdMap + (current -> newInstructions.length), newInstructions :+ source)

      case transformer: TransformerNode =>
        (newIdMap + (current -> (newInstructions.length + 1)),
          newInstructions ++ Seq(transformer, TransformerApplyNode(newInstructions.length, dataInputs)))

      case delTransformer: DelegatingTransformerNode =>
        val transformerId = newIdMap(fitDeps(current).get)
        (newIdMap + (current -> newInstructions.length),
          newInstructions :+ TransformerApplyNode(transformerId, dataInputs))

      case est: EstimatorNode =>
        (newIdMap + (current -> (newInstructions.length + 1)),
          newInstructions ++ Seq(est, EstimatorFitNode(newInstructions.length, dataInputs)))
    }
  }

  /**
   * Get the set of all instruction ids depending on the result of a given instruction
   * (including transitive dependencies)
   *
   * @param id
   * @param instructions
   * @return
   */
  def getChildren(id: Int, instructions: Seq[Instruction]): Set[Int] = {
    val children = scala.collection.mutable.Set[Int]()

    // Todo: Can optimize by looking at only instructions > id
    // Todo: Could also make a more optimized implementation
    // by calculating it for all instructions at once
    for ((instruction, index) <- instructions.zipWithIndex) {
      if (instruction.getDependencies.exists { x =>
        children.contains(x) || x == id
      }) {
        children.add(index)
      }
    }

    children.toSet
  }

  /**
   * Get the set of all instruction ids with the result of a given instruction in their
   * direct dependencies. (Does not include transitive dependencies)
   *
   * Note: This does not capture the number of times that each instruction depends on
   * the input id
   *
   * @param id
   * @param instructions
   * @return
   */
  def getImmediateChildren(id: Int, instructions: Seq[Instruction]): Set[Int] = {
    // Todo: Can optimize by looking at only instructions > id
    // Todo: Could also make a more optimized implementation
    // by calculating it for all instructions at once
    instructions.indices.filter {
      i => instructions(i).getDependencies.contains(id)
    }.toSet
  }

  /**
   * Get the set of all instruction ids on whose results a given instruction depends
   *
   * @param id
   * @param instructions
   * @return
   */
  def getParents(id: Int, instructions: Seq[Instruction]): Set[Int] = {
    // Todo: Could make a more optimized implementation
    // by calculating it for all instructions at once
    val dependencies = if (id != Pipeline.SOURCE) instructions(id).getDependencies else Seq()
    dependencies.map {
      parent => getParents(parent, instructions) + parent
    }.fold(Set())(_ union _)
  }

  def numPerPartition[T](rdd: RDD[T]): Map[Int, Int] = {
    rdd.mapPartitionsWithIndex {
      case (id, partition) => Iterator.single((id, partition.length))
    }.collect().toMap
  }
}
