package workflow

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
}
