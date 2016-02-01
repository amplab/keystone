package workflow

import org.apache.spark.rdd.RDD

/**
 * Node-level optimization, such as selecting a Linear Solver
 *
 * @param sampleFraction The fraction of the RDD to use for operations
 */
class NodeOptimizationRule(sampleFraction: Double = 0.01, seed: Long = 0) extends Rule {
  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)

    // First, figure out which instructions we should actually execute.
    // Take the set of all instructions that are parents of an optimizable node transform or fit instruction.
    // And, keep those that do not depend on the runtime input of a fit pipeline
    val instructionsToExecute = instructions.zipWithIndex.map {
      case (TransformerApplyNode(tIndex, _), index) =>
        instructions(tIndex) match {
          case _: OptimizableTransformer[_, _] => WorkflowUtils.getParents(index, instructions) + index
          case _ => Set[Int]()
        }

      case (EstimatorFitNode(estIndex, _), index) =>
        instructions(estIndex) match {
          case _: OptimizableEstimator[_, _] => WorkflowUtils.getParents(index, instructions) + index
          case _: OptimizableLabelEstimator[_, _, _] => WorkflowUtils.getParents(index, instructions) + index
          case _ => Set[Int]()
        }

      case _ => Set[Int]()
    }.reduce(_ union _) -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions) - Pipeline.SOURCE

    // Execute the minimal amount necessary of the pipeline on sampled nodes, and optimize the optimizable nodes
    val optimizedInstructions = instructions.toArray
    val registers = new Array[Any](instructions.length)
    val numPerPartitionPerNode = Array.fill[Option[Map[Int, Int]]](instructions.length)(None)

    for ((instruction, index) <- instructions.zipWithIndex) {
      if (instructionsToExecute.contains(index)) {
        instruction match {
          case SourceNode(rdd) =>
            val sampledRDD = rdd.sample(withReplacement = false, sampleFraction, seed)
            registers(index) = sampledRDD
            numPerPartitionPerNode(index) = Some(WorkflowUtils.numPerPartition(rdd))

          case transformer: TransformerNode =>
            registers(index) = transformer

          case estimator: EstimatorNode =>
            registers(index) = estimator

          case TransformerApplyNode(tIndex, inputIndices) =>
            val numPerPartition = numPerPartitionPerNode(inputIndices.head)
            val inputs = inputIndices.map(registers(_).asInstanceOf[RDD[_]].cache())
            inputs.foreach(_.count())
            val transformer = registers(tIndex).asInstanceOf[TransformerNode]

            // Optimize the transformer if possible
            val optimizedTransformer = transformer match {
              case ot: OptimizableTransformer[a, b] =>
                ot.optimize(inputs.head.asInstanceOf[RDD[a]], numPerPartition.get)
              case _ => transformer
            }

            registers(index) = optimizedTransformer.transformRDD(inputs)
            optimizedInstructions(tIndex) = optimizedTransformer
            numPerPartitionPerNode(index) = numPerPartition

          case EstimatorFitNode(estIndex, inputIndices) =>
            val numPerPartition = numPerPartitionPerNode(inputIndices.head)
            val inputs = inputIndices.map(registers(_).asInstanceOf[RDD[_]].cache())
            inputs.foreach(_.count())
            val estimator = registers(estIndex).asInstanceOf[EstimatorNode]

            // Optimize the estimator if possible
            val optimizedEstimator = estimator match {
              case ot: OptimizableEstimator[a, b] =>
                ot.optimize(inputs.head.asInstanceOf[RDD[a]], numPerPartition.get)
              case ot: OptimizableLabelEstimator[a, b, l] =>
                ot.optimize(inputs.head.asInstanceOf[RDD[a]], inputs(1).asInstanceOf[RDD[l]], numPerPartition.get)
              case _ => estimator
            }

            registers(index) = optimizedEstimator.fitRDDs(inputs)
            optimizedInstructions(estIndex) = optimizedEstimator
            numPerPartitionPerNode(index) = numPerPartition
        }
      }
    }

    WorkflowUtils.instructionsToPipeline(optimizedInstructions)
  }
}
