package workflow

import org.apache.spark.rdd.RDD

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
            case estimatorFitNode: EstimatorFitNode => {
              instructionIdToNodeId.put(instruction, nodes.length)
              // TODO: Get more reasonable label...
              nodes.append(new DelegatingTransformerNode("Estimator fit"))
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
    var curIdMap = nodeIdToInstructionId
    var curInstructions = instructions

    for (dep <- fitDeps(current)) {
      if (!curIdMap.contains(dep) && dep != Pipeline.SOURCE) {
        val (newIdMap, newInstructions) = pipelineToInstructionsRecursion(dep, nodes, dataDeps, fitDeps, curIdMap, curInstructions)
        curIdMap = newIdMap
        curInstructions = newInstructions
      }
    }

    for (dep <- dataDeps(current)) {
      if (!curIdMap.contains(dep) && dep != Pipeline.SOURCE) {
        val (newIdMap, newInstructions) = pipelineToInstructionsRecursion(dep, nodes, dataDeps, fitDeps, curIdMap, curInstructions)
        curIdMap = newIdMap
        curInstructions = newInstructions
      }
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

  /**
   * Get the set of all instruction ids depending on the result of a given instruction
   *
   * @param id
   * @param instructions
   * @return
   */
  def getChildren(id: Int, instructions: Seq[Instruction]): Set[Int] = {
    val children = scala.collection.mutable.Set[Int]()

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
   * Get the set of all instruction ids on whose results a given instruction depends
   *
   * @param id
   * @param instructions
   * @return
   */
  def getParents(id: Int, instructions: Seq[Instruction]): Set[Int] = {
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
