package workflow


object WorkflowUtils {
  def pipelineToInstructions[A, B](pipeline: Pipeline[A, B]): Seq[Instruction] = {
    val nodes = pipeline.nodes
    val dataDeps = pipeline.dataDeps
    val fitDeps = pipeline.fitDeps
    val sink = pipeline.sink

    pipelineRecurse(sink, nodes, dataDeps, fitDeps, Map(Pipeline.SOURCE -> Pipeline.SOURCE), Seq())._2
  }

  def pipelineRecurse(
    current: Int,
    nodes: Seq[Node],
    dataDeps: Seq[Seq[Int]],
    fitDeps: Seq[Option[Int]],
    nodeIdToInstructionId: Map[Int, Int],
    instructions: Seq[Instruction]
  ): (Map[Int, Int], Seq[Instruction]) = {
    var curIdMap = nodeIdToInstructionId
    var curInstructions = instructions
    for (dep <- fitDeps(current).filter(_ != Pipeline.SOURCE).filter(!nodeIdToInstructionId.contains(_))) {
      val (newIdMap, newInstructions) = pipelineRecurse(dep, nodes, dataDeps, fitDeps, curIdMap, curInstructions)
      curIdMap = newIdMap
      curInstructions = newInstructions
    }

    for (dep <- dataDeps(current).filter(_ != Pipeline.SOURCE).filter(!nodeIdToInstructionId.contains(_))) {
      val (newIdMap, newInstructions) = pipelineRecurse(dep, nodes, dataDeps, fitDeps, curIdMap, curInstructions)
      curIdMap = newIdMap
      curInstructions = newInstructions
    }

    nodes(current) match {
      case source: SourceNode => {
        curIdMap = curIdMap + (current -> curInstructions.length)
        curInstructions = curInstructions :+ source
        (curIdMap, curInstructions)
      }

      case transformer: TransformerNode => {
        curInstructions = curInstructions :+ transformer
        val inputs = dataDeps(current).map(curIdMap.apply)
        curIdMap = curIdMap + (current -> curInstructions.length)
        curInstructions = curInstructions :+ TransformerApplyNode(curInstructions.length - 1, inputs)
        (curIdMap, curInstructions)
      }

      case delTransformer: DelegatingTransformerNode => {
        val transformerId = curIdMap(fitDeps(current).get)
        val dataInputs = dataDeps(current).map(curIdMap.apply)
        curIdMap = curIdMap + (current -> curInstructions.length)
        curInstructions = curInstructions :+ TransformerApplyNode(transformerId, dataInputs)
        (curIdMap, curInstructions)
      }

      case est: EstimatorNode => {
        curInstructions = curInstructions :+ est
        val inputs = dataDeps(current).map(curIdMap.apply)
        curIdMap = curIdMap + (current -> curInstructions.length)
        curInstructions = curInstructions :+ EstimatorFitNode(curInstructions.length - 1, inputs)
        (curIdMap, curInstructions)
      }
    }

  }
}
