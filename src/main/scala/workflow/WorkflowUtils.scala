package workflow

import scala.collection.mutable.ArrayBuffer

object WorkflowUtils {
  def instructionsToPipeline[A, B](instructions: Seq[Instruction]): Pipeline[A, B] = {
    val nodes = new ArrayBuffer[Node]()
    val dataDeps = new ArrayBuffer[Seq[Int]]()
    val fitDeps = new ArrayBuffer[Option[Int]]()
    val instructionIdToNodeId = scala.collection.mutable.Map.empty[Int, Int]
    instructionIdToNodeId.put(Pipeline.SOURCE, Pipeline.SOURCE)

    for (instruction <- instructions.indices) {
      instructions(instruction) match {
        case est: EstimatorNode => Unit
        case transformer: TransformerNode => Unit
        case source: SourceNode => {
          instructionIdToNodeId.put(instruction, nodes.length)
          nodes.append(source)
          dataDeps.append(Seq())
          fitDeps.append(None)
        }
        case TransformerApplyNode(transformer, inputs) => {
          instructions(transformer) match {
            case transformerNode: TransformerNode => {
              instructionIdToNodeId.put(instruction, nodes.length)
              nodes.append(transformerNode)
              dataDeps.append(inputs.map(instructionIdToNodeId.apply))
              fitDeps.append(None)
            }
            case EstimatorFitNode(est, estInputs) => {
              instructionIdToNodeId.put(instruction, nodes.length)

              val label = s"Fit[${instructions(est).asInstanceOf[EstimatorNode].label}]"
              nodes.append(new DelegatingTransformerNode(label))
              dataDeps.append(inputs.map(instructionIdToNodeId.apply))
              fitDeps.append(Some(instructionIdToNodeId(transformer)))
            }
            case _ => throw new RuntimeException("Transformer apply instruction must point at a Transformer")
          }
        }
        case EstimatorFitNode(est, inputs) => {
          val estimatorNode = instructions(est).asInstanceOf[EstimatorNode]
          instructionIdToNodeId.put(instruction, nodes.length)
          nodes.append(estimatorNode)
          dataDeps.append(inputs.map(instructionIdToNodeId.apply))
          fitDeps.append(None)
        }
      }
    }

    new ConcretePipeline(nodes.toSeq, dataDeps.toSeq, fitDeps.toSeq, nodes.length - 1)
  }

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
