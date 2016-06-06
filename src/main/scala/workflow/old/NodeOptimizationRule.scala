package workflow.old

import org.apache.spark.rdd.RDD

/**
 * Node-level optimization, such as selecting a Linear Solver
 *
 * @param samplesPerPartition The number of items per partition to look at when optimizing nodes
 */
class NodeOptimizationRule(samplesPerPartition: Int = 3) extends Rule {
  case class OptimizationState(
    optimizedInstructions: Seq[Instruction],
    unoptimizedInstructionIndexToOptimizedIndex: Int => Int,
    registers: Map[Int, InstructionOutput],
    numPerPartitionPerNode: Map[Int, Map[Int, Int]]
  )

  /**
   * Get the set of instructions that we need to check for optimizations at.
   *
   * This is any instruction that either applies an optimizable transformer,
   * or fits an optimizable estimator,
   * and is not supposed to be executed on test data.
   */
  private def getInstructionsToOptimize(instructions: Seq[Instruction]): Set[Int] = {
    instructions.zipWithIndex.collect {
      case (TransformerApplyNode(tIndex, _), index)
        if instructions(tIndex).isInstanceOf[OptimizableTransformer[_, _]] => index

      case (EstimatorFitNode(estIndex, _), index)
        if instructions(estIndex).isInstanceOf[OptimizableEstimator[_, _]] => index

      case (EstimatorFitNode(estIndex, _), index)
        if instructions(estIndex).isInstanceOf[OptimizableLabelEstimator[_, _, _]] => index

    }.toSet -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions)
  }

  /**
   * Execute the instruction at the given index, and output the new state of the optimization process
   *
   * @param optimizationState
   * @param instruction
   * @param index
   * @return
   */
  private def executeInstruction(optimizationState: OptimizationState, instruction: Instruction, index: Int)
  : OptimizationState = {
    // Get the dependencies from the registers and run the instruction on them (caching RDD outputs)
    val registers = optimizationState.registers
    val deps = instruction.getDependencies.map(registers)
    val instructionOutput = instruction.execute(deps) match {
      case RDDOutput(rdd) => RDDOutput(rdd.cache())
      case out => out
    }
    val newRegisters = registers + (index -> instructionOutput)

    // If any of the dependencies is an RDD and has a numPerPartition, make sure to feed it forward
    val numPerPartitionPerNode = optimizationState.numPerPartitionPerNode
    val numPerPartition = instruction.getDependencies.collectFirst {
      case dep if numPerPartitionPerNode.contains(dep) => numPerPartitionPerNode(dep)
    }
    val newNumPerPartitionPerNode = if (numPerPartition.nonEmpty) {
      numPerPartitionPerNode + (index -> numPerPartition.get)
    } else {
      numPerPartitionPerNode
    }

    // Create the new state
    optimizationState.copy(registers = newRegisters, numPerPartitionPerNode = newNumPerPartitionPerNode)
  }

  /**
   * Optimizes the transformer being applied by the instruction at the given index.
   * Outputs a new optimization state.
   *
   * @param optState The previous optimization state
   * @param transformerApplyNode
   * @param index
   * @return
   */
  private def optimizeTransformer(optState: OptimizationState, transformerApplyNode: TransformerApplyNode, index: Int)
  : OptimizationState = {
    val tIndex = transformerApplyNode.transformer
    val inputIndices = transformerApplyNode.inputs

    // Here we assume that if a transformer takes multiple RDDs as input,
    // all of them are copartitioned and have the exact same number of partitions,
    // and the same number of values in each partition
    val numPerPartition = optState.numPerPartitionPerNode(inputIndices.head)
    val inputs = inputIndices.map(optState.registers).collect {
      case RDDOutput(rdd) => rdd
    }

    // Optimize the transformer if possible
    optState.registers(tIndex) match {
      case TransformerOutput(ot: OptimizableTransformer[a, b]) => {
        // Actually optimize the transformer based on a data sample and statistics about the data partitions.
        //
        // Then, get the partial pipeline that is the output of the optimization, and prepare to splice it
        // into the optimized instructions
        val pipeToSplice = ot.optimize(inputs.head.asInstanceOf[RDD[a]], numPerPartition)
        val instructionsToSplice = WorkflowUtils.pipelineToInstructions(pipeToSplice)

        // First: Disconnect all the dependencies in the optimized pipeline on this TransformerApplyNode,
        // and replace them with the spliceEndpoint
        val spliceEndpoint = Pipeline.SOURCE - 1
        val indexInNewPipeline = optState.unoptimizedInstructionIndexToOptimizedIndex(index)
        val removeResult = WorkflowUtils.disconnectAndRemoveInstructions(
          Map(indexInNewPipeline -> spliceEndpoint),
          optState.optimizedInstructions)
        val intermediateOptimizedInstructions = removeResult._1
        val intermediateIndexMapping = optState.unoptimizedInstructionIndexToOptimizedIndex andThen removeResult._2

        // Then: Splice in the transformer's optimization into the optimized pipeline
        // Note: We do not delete the optimizable transformer because it may be used elsewhere
        // if we've merged equivalent TransformerNodes.
        val spliceResult = WorkflowUtils.spliceInstructions(
          instructionsToSplice,
          intermediateOptimizedInstructions,
          Map(Pipeline.SOURCE -> intermediateIndexMapping(inputIndices.head)),
          spliceEndpoint)

        optState.copy(
          optimizedInstructions = spliceResult._1,
          unoptimizedInstructionIndexToOptimizedIndex = intermediateIndexMapping andThen spliceResult._2
        )
      }
      case _: TransformerOutput => optState
      case _ => throw new ClassCastException("TransformerApplyNode dep wasn't pointing at a transformer")
    }
  }

  /**
   * Optimizes the estimator being fit by the instruction at the given index.
   * Outputs a new optimization state.
   *
   * @param optState The previous optimization state
   * @param estimatorFitNode
   * @param instructions
   * @param index
   * @return
   */
  private def optimizeEstimator(
      optState: OptimizationState,
      estimatorFitNode: EstimatorFitNode,
      instructions: Seq[Instruction],
      index: Int
    ): OptimizationState = {
    val inputIndices = estimatorFitNode.inputs
    val estIndex = estimatorFitNode.est

    val numPerPartition = optState.numPerPartitionPerNode(inputIndices.head)
    val inputs = inputIndices.map(optState.registers).collect {
      case RDDOutput(rdd) => rdd
    }

    // Optimize the estimator if possible
    optState.registers(estIndex) match {
      case EstimatorOutput(oe: OptimizableEstimator[a, b]) => {
        // We start by setting a constant to refer to endpoint 'ids'.
        // The exact value does not matter, the only critical thing
        // is that the value does not refer to any existing instructions
        // or to the source
        val spliceEndpoint = Pipeline.SOURCE - 1

        // Actually optimize the estimator based on a data sample and statistics about the data partitions.
        // Then, extract the optimized instructions that we need to splice into the pipeline
        val dataSample = inputs.head.asInstanceOf[RDD[a]]
        val pipeToSplice = oe.optimize(dataSample, numPerPartition).apply(dataSample)
        val initialInstructionsToSplice = WorkflowUtils.pipelineToInstructions(pipeToSplice)
        val dataSampleIndices = initialInstructionsToSplice.indices.filter { inx =>
          initialInstructionsToSplice(inx) == SourceNode(dataSample)
        }.toSet
        val instructionsToSplice = WorkflowUtils.disconnectAndRemoveInstructions(
          dataSampleIndices.map(_ -> spliceEndpoint).toMap,
          initialInstructionsToSplice
        )._1

        // First: Disconnect all the dependencies in the optimized pipeline on this existing EstimatorFitNode,
        // and remove this node
        val indexInNewPipeline = optState.unoptimizedInstructionIndexToOptimizedIndex(index)
        val removeResult = WorkflowUtils.disconnectAndRemoveInstructions(
          Map(indexInNewPipeline -> (spliceEndpoint - 1)),
          optState.optimizedInstructions)
        var partialOptimizedInstructions = removeResult._1
        var partialOldIndexToNewIndex = optState.unoptimizedInstructionIndexToOptimizedIndex andThen removeResult._2

        // Then, insert the optimized fit & transform instructions to apply every place in the pipeline
        // that was previously applying the output of this estimatorFitNode to new data
        instructions.zipWithIndex.foreach {
          case (TransformerApplyNode(t, tInputs), transformerApplyIndex) if t == index => {
            // Disconnect all the dependencies in the optimized pipeline on this TransformerApplyNode,
            // and replace them with the spliceEndpoint
            val indexInNewPipeline = partialOldIndexToNewIndex(transformerApplyIndex)
            val removeResult = WorkflowUtils.disconnectAndRemoveInstructions(
              Map(indexInNewPipeline -> spliceEndpoint),
              partialOptimizedInstructions)
            partialOptimizedInstructions = removeResult._1
            partialOldIndexToNewIndex = partialOldIndexToNewIndex andThen removeResult._2

            // Splice in the instructions into the optimized pipeline
            val spliceResult = WorkflowUtils.spliceInstructions(
              instructionsToSplice,
              partialOptimizedInstructions,
              Map(spliceEndpoint -> partialOldIndexToNewIndex(inputIndices.head),
                Pipeline.SOURCE -> partialOldIndexToNewIndex(tInputs.head)),
              spliceEndpoint)
            partialOptimizedInstructions = spliceResult._1
            partialOldIndexToNewIndex = partialOldIndexToNewIndex andThen spliceResult._2
          }

          case _ => Unit
        }

        optState.copy(
          optimizedInstructions = partialOptimizedInstructions,
          unoptimizedInstructionIndexToOptimizedIndex = partialOldIndexToNewIndex
        )
      }

      case EstimatorOutput(oe: OptimizableLabelEstimator[a, b, l]) => {
        // We start by setting constants to refer to endpoint 'ids'.
        // The exact values and ordering do not matter, the only critical thing
        // is that the values are different and do not refer to any existing instructions
        // or to the source
        val dataSpliceEndpoint = Pipeline.SOURCE - 1
        val labelSpliceEndpoint = Pipeline.SOURCE - 2
        val spliceEndpoint = Pipeline.SOURCE - 3

        // Actually optimize the estimator based on a data sample and statistics about the data partitions.
        // Then, extract the optimized instructions that we need to splice into the pipeline
        val dataSample = inputs(0).asInstanceOf[RDD[a]]
        val labelSample = inputs(1).asInstanceOf[RDD[l]]
        val pipeToSplice = oe.optimize(dataSample, labelSample, numPerPartition)
          .apply(dataSample, labelSample)
        val initialInstructionsToSplice = WorkflowUtils.pipelineToInstructions(pipeToSplice)
        val dataSampleIndices = initialInstructionsToSplice.indices.filter { inx =>
          initialInstructionsToSplice(inx) == SourceNode(dataSample)
        }.toSet
        val labelSampleIndices = initialInstructionsToSplice.indices.filter { inx =>
          initialInstructionsToSplice(inx) == SourceNode(labelSample)
        }.toSet
        val instructionsToSplice = WorkflowUtils.disconnectAndRemoveInstructions(
          dataSampleIndices.map(_ -> dataSpliceEndpoint).toMap ++
            labelSampleIndices.map(_ -> labelSpliceEndpoint).toMap,
          initialInstructionsToSplice
        )._1

        // First: Disconnect all the dependencies in the optimized pipeline on this EstimatorFitNode,
        // and remove this node
        val indexInNewPipeline = optState.unoptimizedInstructionIndexToOptimizedIndex(index)
        val removeResult = WorkflowUtils.disconnectAndRemoveInstructions(
          Map(indexInNewPipeline -> (labelSpliceEndpoint - 1)),
          optState.optimizedInstructions)
        var partialOptimizedInstructions = removeResult._1
        var partialOldIndexToNewIndex = optState.unoptimizedInstructionIndexToOptimizedIndex andThen removeResult._2

        // Then, insert the optimized fit & transform instructions to apply every place in the pipeline
        // that was previously applying the output of this estimatorFitNode to new data
        instructions.zipWithIndex.foreach {
          case (TransformerApplyNode(t, tInputs), transformerApplyIndex) if t == index => {
            // Disconnect all the dependencies in the optimized pipeline on this TransformerApplyNode,
            // and replace them with the spliceEndpoint
            val indexInNewPipeline = partialOldIndexToNewIndex(transformerApplyIndex)
            val removeResult = WorkflowUtils.disconnectAndRemoveInstructions(
              Map(indexInNewPipeline -> spliceEndpoint),
              partialOptimizedInstructions)
            partialOptimizedInstructions = removeResult._1
            partialOldIndexToNewIndex = partialOldIndexToNewIndex andThen removeResult._2

            // Splice in the instructions into the optimized pipeline
            val spliceResult = WorkflowUtils.spliceInstructions(
              instructionsToSplice,
              partialOptimizedInstructions,
              Map(
                dataSpliceEndpoint -> partialOldIndexToNewIndex(inputIndices(0)),
                labelSpliceEndpoint -> partialOldIndexToNewIndex(inputIndices(1)),
                Pipeline.SOURCE -> partialOldIndexToNewIndex(tInputs.head)
              ),
              spliceEndpoint)
            partialOptimizedInstructions = spliceResult._1
            partialOldIndexToNewIndex = partialOldIndexToNewIndex andThen spliceResult._2
          }

          case _ => Unit
        }

        optState.copy(
          optimizedInstructions = partialOptimizedInstructions,
          unoptimizedInstructionIndexToOptimizedIndex = partialOldIndexToNewIndex
        )
      }

      case EstimatorOutput(e) => optState

      case _ => throw new ClassCastException("EstimatorFitNode dep wasn't pointing at an estimator")
    }
  }

  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)

    // First, figure out which instructions we should actually optimize and execute.
    // Execute the set of all instructions that are parents of an optimizable node transform or fit instruction.
    // And, keep those that do not depend on the runtime input of a fit pipeline
    val instructionsToOptimize = getInstructionsToOptimize(instructions)

    val instructionsToExecute = instructionsToOptimize.map {
      index => WorkflowUtils.getParents(index, instructions)
    }.fold(Set[Int]())(_ union _)

    val initialState = OptimizationState(instructions, identity, Map(), Map())

    // Execute the minimal amount necessary of the pipeline on sampled nodes, and optimize the optimizable nodes
    val finalOptimization = instructions.zipWithIndex.foldLeft[OptimizationState](initialState) {
      case (optimizationState, (tApplyNode @ TransformerApplyNode(_, _), index))
        if instructionsToOptimize.contains(index) => {
          val newOptimizationState = optimizeTransformer(optimizationState, tApplyNode, index)
          if (instructionsToExecute.contains(index)) {
            executeInstruction(newOptimizationState, tApplyNode, index)
          } else {
            newOptimizationState
          }
        }

      case (optimizationState, (estFitNode @ EstimatorFitNode(_, _), index))
        if instructionsToOptimize.contains(index) => {
          val newOptimizationState = optimizeEstimator(optimizationState, estFitNode, instructions, index)
          if (instructionsToExecute.contains(index)) {
            executeInstruction(newOptimizationState, estFitNode, index)
          } else {
            newOptimizationState
          }
        }

      case (optimizationState, (SourceNode(rdd), index)) if instructionsToExecute.contains(index) => {
        // Sample the RDD (with a value copy to avoid serializing this class when doing mapPartitions)
        val spp = samplesPerPartition
        val sampledRDD = rdd.mapPartitions(_.take(spp))
        val executedState = executeInstruction(optimizationState, SourceNode(sampledRDD), index)

        // Make sure to set numPerPartition for this node
        val numPerPartition = WorkflowUtils.numPerPartition(rdd)
        executedState.copy(numPerPartitionPerNode = executedState.numPerPartitionPerNode + (index -> numPerPartition))
      }

      case (optimizationState, (instruction, index)) if instructionsToExecute.contains(index) => {
        executeInstruction(optimizationState, instruction, index)
      }

      case (optimizationState, _) => {
        optimizationState
      }
    }

    // Unpersist any RDDs we cached
    finalOptimization.registers.values.foreach {
      case RDDOutput(rdd) => rdd.unpersist()
      case _ => Unit
    }

    WorkflowUtils.instructionsToPipeline(finalOptimization.optimizedInstructions)
  }
}
