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
    var optimizedInstructions = instructions
    var oldIndexToNewIndex: Int => Int = instructions.indices.zipWithIndex.toMap
    val registers = new Array[InstructionOutput](instructions.length)
    val numPerPartitionPerNode = Array.fill[Option[Map[Int, Int]]](instructions.length)(None)

    for ((instruction, index) <- instructions.zipWithIndex) {
      if (instructionsToExecute.contains(index)) {
        instruction match {
          case SourceNode(rdd) => {
            val sampledRDD = rdd.sample(withReplacement = false, sampleFraction, seed)
            registers(index) = RDDOutput(sampledRDD)
            numPerPartitionPerNode(index) = Some(WorkflowUtils.numPerPartition(rdd))
          }

          case TransformerApplyNode(tIndex, inputIndices) => {
            val numPerPartition = numPerPartitionPerNode(inputIndices.head)
            val inputs = inputIndices.map(registers).collect {
              case RDDOutput(rdd) => rdd.cache()
            }
            inputs.foreach(_.count())

            // Optimize the transformer if possible
            val transformer = registers(tIndex) match {
              case TransformerOutput(ot: OptimizableTransformer[a, b]) =>
                val pipeToSplice = ot.optimize(inputs.head.asInstanceOf[RDD[a]], numPerPartition.get)
                val instructionsToSplice = WorkflowUtils.pipelineToInstructions(pipeToSplice)

                // First: Disconnect all the dependencies in the optimized pipeline on this TransformerApplyNode
                val spliceEndpoint = Pipeline.SOURCE - 1
                val indexInNewPipeline = oldIndexToNewIndex(index)
                optimizedInstructions = optimizedInstructions.map(_.mapDependencies {
                  i => if (i == indexInNewPipeline) spliceEndpoint else i
                })

                // Next: Remove this TransformerApplyNode from the optimized pipeline
                val removeResult = WorkflowUtils.removeInstructions(Set(indexInNewPipeline), optimizedInstructions)
                optimizedInstructions = removeResult._1
                oldIndexToNewIndex = oldIndexToNewIndex andThen removeResult._2

                // Finally: Splice in the transformer's optimization into the optimized pipeline
                // Note: We do not delete the optimizable transformer because it may be used elsewhere
                // if we've merged equivalent TransformerNodes.
                val spliceResult = WorkflowUtils.spliceInstructions(
                  instructionsToSplice,
                  optimizedInstructions,
                  Map(Pipeline.SOURCE -> oldIndexToNewIndex(inputIndices.head)),
                  spliceEndpoint)
                optimizedInstructions = spliceResult._1
                oldIndexToNewIndex = oldIndexToNewIndex andThen spliceResult._2

                ot
              case TransformerOutput(t) => t
              case _ => throw new ClassCastException("TransformerApplyNode dep wasn't pointing at a transformer")
            }

            registers(index) = RDDOutput(transformer.transformRDD(inputs))
            numPerPartitionPerNode(index) = numPerPartition
          }

          case EstimatorFitNode(estIndex, inputIndices) => {
            val numPerPartition = numPerPartitionPerNode(inputIndices.head)
            val inputs = inputIndices.map(registers).collect {
              case RDDOutput(rdd) => rdd.cache()
            }
            inputs.foreach(_.count())

            // Optimize the estimator if possible
            val estimator = registers(estIndex) match {
              case EstimatorOutput(oe: OptimizableEstimator[a, b]) =>
                // Get the instructions to splice into the optimized pipeline
                val dataSample = inputs.head.asInstanceOf[RDD[a]]
                val pipeToSplice = oe.optimize(dataSample, numPerPartition.get).apply(dataSample)
                val initialInstructionsToSplice = WorkflowUtils.pipelineToInstructions(pipeToSplice)
                val dataSampleIndices = initialInstructionsToSplice.indices.filter {
                  initialInstructionsToSplice(_) == SourceNode(dataSample)
                }.toSet
                val instructionsToSplice = WorkflowUtils.removeInstructions(dataSampleIndices,
                  initialInstructionsToSplice.map(_.mapDependencies {
                    i => if (dataSampleIndices.contains(i)) Pipeline.SOURCE - 1 else i
                  }))

                // Get the set of nodes depending on this estimator fit node,
                // and disconnect them from it

                oe
              case EstimatorOutput(oe: OptimizableLabelEstimator[a, b, l]) =>
                oe.optimize(inputs(0).asInstanceOf[RDD[a]], inputs(1).asInstanceOf[RDD[l]], numPerPartition.get)
                oe
              case EstimatorOutput(e) => e
              case _ => throw new ClassCastException("EstimatorFitNode dep wasn't pointing at an estimator")
            }

            registers(index) = TransformerOutput(estimator.fitRDDs(inputs))
            numPerPartitionPerNode(index) = numPerPartition
          }

          case node: Instruction => {
            val deps = node.getDependencies.map(registers)
            registers(index) = node.execute(deps)
          }
        }
      }
    }

    WorkflowUtils.instructionsToPipeline(optimizedInstructions)
  }
}
