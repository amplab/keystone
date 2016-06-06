package workflow

import breeze.linalg.{max, DenseVector, DenseMatrix}
import nodes.util.Cacher
import org.apache.spark.util.SparkUtilWrapper
import pipelines.Logging
import workflow.AutoCacheRule.{GreedyCache, AggressiveCache, CachingStrategy}

case class Profile(ns: Long, rddMem: Long, driverMem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.rddMem + p.rddMem, this.driverMem + p.driverMem)
}

case class SampleProfile(scale: Long, profile: Profile)

class AutoCacheRule(cachingMode: CachingStrategy) extends Rule with Logging {

  /**
   * Get the node weights: estimates for how many passes an instruction will make over its input dependencies
   */
  def getNodeWeights(instructions: Seq[Instruction]): Map[Int, Int] = {
    instructions.zipWithIndex.map {
      case (TransformerApplyNode(transformer, _), i) => instructions(transformer) match {
        case node: WeightedNode => (i, node.weight)
        case _ => (i, 1)
      }
      case (EstimatorFitNode(estimator, _), i) => instructions(estimator) match {
        case node: WeightedNode => (i, node.weight)
        case _ => (i, 1)
      }
      case (_, i) => (i, 1)
    }.toMap
  }

  /**
   * Get a map representing the immediate children for each instruction
   * Note: This doesn't capture how many times each child depended on the instruction
   */
  def getImmediateChildrenByInstruction(instructions: Seq[Instruction]): Map[Int, Seq[Int]] = {
    instructions.indices.map(i => (i, WorkflowUtils.getImmediateChildren(i, instructions))).toMap
  }

  /**
   * Get an estimate for how many times each instruction will be executed, assuming
   * the given set of instructions have their outputs cached
   */
  def getRuns(
      instructions: Seq[Instruction],
      immediateChildrenByInstruction: Map[Int, Seq[Int]],
      cache: Set[Int],
      nodeWeights: Map[Int, Int]
    ): Array[Int] = {
    val runsByIndex = new Array[Int](instructions.size)
    var i = instructions.size - 1
    while (i >= 0) {
      if (immediateChildrenByInstruction(i).isEmpty) {
        runsByIndex(i) = 1
      }
      else {
        val runs = immediateChildrenByInstruction(i).map(j => if (cache.contains(j)) {
          nodeWeights(j)
        } else {
          nodeWeights(j) * runsByIndex(j)
        }).sum
        runsByIndex(i) = runs
      }

      i = i - 1
    }

    runsByIndex
  }

  /**
   * Get the initial set of what instructions will have their results effectively cached
   */
  def initCacheSet(instructions: Seq[Instruction]): Set[Int] = {
    instructions.indices.filter { instructions(_) match {
      case _: EstimatorNode => true
      case _: TransformerNode =>  true
      case _: EstimatorFitNode => true
      case _: SourceNode => false
      case TransformerApplyNode(t, _) => instructions(t).isInstanceOf[Cacher[_]]
    }}.toSet
  }

  /**
   * This method takes a sequence of profiles at different sample sizes,
   * and generalizes them to a new data scale by fitting then using linear models
   * for memory and cpu usage dependent on data scale.
   */
  def generalizeProfiles(newScale: Long, sampleProfiles: Seq[SampleProfile]): Profile = {
    def getModel(inp: Iterable[(Long, String, Long)]): Double => Double = {
      val observations = inp.toArray

      //Pack a data matrix with observations
      val X = DenseMatrix.ones[Double](observations.length, 2)
      observations.zipWithIndex.foreach(o => X(o._2, 0) = o._1._1.toDouble)
      val y = DenseVector(observations.map(_._3.toDouble))
      // We solve a linear model for how either memory or time varies with respect to data size
      // (We train separate models for time and memory usage)
      val model = max(X \ y, 0.0)

      //A function to apply the model.
      def res(x: Double): Double = DenseVector(x, 1.0).t * model

      res
    }

    val samples = sampleProfiles.flatMap { case SampleProfile(scale, value) =>
      Array(
        (scale, "memory", value.rddMem),
        (scale, "time", value.ns),
        (scale, "driverMem", value.driverMem)
      )}.groupBy(a => a._2)

    val models = samples.mapValues(getModel)

    Profile(
      models("time").apply(newScale).toLong,
      models("memory").apply(newScale).toLong,
      models("driverMem").apply(newScale).toLong)
  }

  /**
   * Get a profile of each node in the pipeline
   *
   * @param instructions The pipeline instructions
   * @param partitionScales The scales to profile at (expected number of data points per partition)
   * @param numTrials The number of times to profile at each scale
   * @return
   */
  def profileInstructions(
    instructions: Seq[Instruction],
    partitionScales: Seq[Long],
    numTrials: Int
  ): Map[Int, Profile] = {
    val cached = initCacheSet(instructions)
    val nodeWeights = getNodeWeights(instructions)
    val immediateChildrenByInstruction = getImmediateChildrenByInstruction(instructions)
    val runs = getRuns(instructions, immediateChildrenByInstruction, cached, nodeWeights)
    // We have the possibility of caching all uncached nodes accessed more than once,
    // That don't depend on the test data.
    val instructionsToProfile = instructions.indices.toSet
      .filter(i => !cached(i))
      .filter(i => runs(i) > 1) -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions)

    // We need to execute all instructions that the nodes we need to profile depend on.
    val instructionsToExecute = instructionsToProfile
      .map(i => WorkflowUtils.getParents(i, instructions) + i)
      .fold(Set())(_ ++ _)

    val registers = new Array[InstructionOutput](instructions.length)
    val numPerPartitionPerNode = scala.collection.mutable.Map[Int, Map[Int, Int]]()
    val profiles = scala.collection.mutable.Map[Int, Profile]()

    val sortedScales = partitionScales.sorted
    for ((instruction, i) <- instructions.zipWithIndex if instructionsToExecute.contains(i)) {
      if (instructionsToProfile.contains(i)) {
        instruction match {
          case SourceNode(rdd) => {
            val npp = WorkflowUtils.numPerPartition(rdd)
            numPerPartitionPerNode(i) = npp

            val totalCount = npp.values.map(_.toLong).sum

            val sampleProfiles = for (
              (partitionScale, scaleIndex) <- sortedScales.zipWithIndex;
              trial <- 1 to numTrials
            ) yield {
              // Calculate the necessary number of items per partition to maintain the same partition distribution,
              // while only having scale items instead of totalCount items.
              // Can't use mapValues because that isn't serializable
              val scale = partitionScale * npp.size
              val scaledNumPerPartition = npp.toSeq
                .map(x => (x._1, math.round((scale.toDouble / totalCount) * x._2).toInt)).toMap

              // Profile sample timing
              val start = System.nanoTime()
              // Construct a sample containing only scale items, but w/ the same relative partition distribution
              val sample = rdd.mapPartitionsWithIndex {
                case (pid, partition) => partition.take(scaledNumPerPartition(pid))
              }.cache()
              sample.count()
              val duration = System.nanoTime() - start

              // Profile sample memory
              val rddSize = sample.context.getRDDStorageInfo.filter(_.id == sample.id).map(_.memSize).head

              // If this sample was computed using the final and largest scale, add it to the registers
              if ((scaleIndex == (sortedScales.length - 1)) && (trial == numTrials)) {
                registers(i) = RDDOutput(sample)
              } else {
                sample.unpersist()
              }

              SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, rddSize, 0))
            }

            profiles(i) = generalizeProfiles(totalCount, sampleProfiles)
          }

          case TransformerApplyNode(tIndex, inputIndices) => {
            // We assume that all input rdds to this transformer have equal, zippable partitioning
            val npp = numPerPartitionPerNode(inputIndices.head)
            numPerPartitionPerNode(i) = npp
            val totalCount = npp.values.map(_.toLong).sum

            val transformer = registers(tIndex) match {
              case TransformerOutput(t) => t
              case _ => throw new ClassCastException("TransformerApplyNode dep wasn't pointing at a transformer")
            }
            val inputs = inputIndices.map(registers).collect {
              case RDDOutput(rdd) => rdd.cache()
            }
            inputs.foreach(_.count())

            val sampleProfiles = for (
              (partitionScale, scaleIndex) <- sortedScales.zipWithIndex;
              trial <- 1 to numTrials
            ) yield {
              // Calculate the necessary number of items per partition to maintain the same partition distribution,
              // while only having scale items instead of totalCount items.
              // Can't use mapValues because that isn't serializable
              val scale = partitionScale * npp.size
              val scaledNumPerPartition = npp
                .toSeq.map(x => (x._1, math.round((scale.toDouble / totalCount) * x._2).toInt)).toMap

              // Sample the inputs. Samples containing only scale items, but w/ the same relative partition distribution
              // NOTE: Assumes all inputs have equal, zippable partition counts
              val sampledInputs = inputs.map(_.mapPartitionsWithIndex {
                case (pid, partition) => partition.take(scaledNumPerPartition(pid))
              })

              // Profile sample timing
              val start = System.nanoTime()
              // Construct and cache a sample
              val sample = transformer.transformRDD(sampledInputs.toIterator).cache()
              sample.count()
              val duration = System.nanoTime() - start

              // Profile sample memory
              val rddSize = sample.context.getRDDStorageInfo.filter(_.id == sample.id).map(_.memSize).head

              // If this sample was computed using the final and largest scale, add it to the registers
              if ((scaleIndex == (sortedScales.length - 1)) && (trial == numTrials)) {
                registers(i) = RDDOutput(sample)
              } else {
                sample.unpersist()
              }

              SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, rddSize, 0))
            }

            profiles(i) = generalizeProfiles(totalCount, sampleProfiles)
          }

          case EstimatorFitNode(eIndex, inputIndices) => {
            // We assume that all input rdds to this transformer have equal, zippable partitioning
            val npp = numPerPartitionPerNode(inputIndices.head)
            numPerPartitionPerNode(i) = npp
            val totalCount = npp.values.map(_.toLong).sum

            val estimator = registers(eIndex) match {
              case EstimatorOutput(e) => e
              case _ => throw new ClassCastException("Estimator fit dep wasn't pointing at an Estimator")
            }
            val inputs = inputIndices.map(registers).collect {
              case RDDOutput(rdd) => rdd.cache()
            }
            inputs.foreach(_.count())

            val sampleProfiles = for (
              (partitionScale, scaleIndex) <- sortedScales.zipWithIndex;
              trial <- 1 to numTrials
            ) yield {
              // Calculate the necessary number of items per partition to maintain the same partition distribution,
              // while only having scale items instead of totalCount items.
              // Can't use mapValues because that isn't serializable
              val scale = partitionScale * npp.size
              val scaledNumPerPartition = npp
                .toSeq.map(x => (x._1, math.round((scale.toDouble / totalCount) * x._2).toInt)).toMap

              // Sample the inputs. Samples containing only scale items, but w/ the same relative partition distribution
              // NOTE: Assumes all inputs have equal, zippable partition counts
              val sampledInputs = inputs.map(_.mapPartitionsWithIndex {
                case (pid, partition) => partition.take(scaledNumPerPartition(pid))
              })

              // Profile sample timing
              val start = System.nanoTime()
              // fit the estimator
              val sampleTransformer = estimator.fitRDDs(sampledInputs.toIterator)
              val duration = System.nanoTime() - start

              // Profile fit transformer memory
              val transformerSize = SparkUtilWrapper.estimateSize(sampleTransformer)

              // If this sample was computed using the final and largest scale, add it to the registers
              if ((scaleIndex == (sortedScales.length - 1)) && (trial == numTrials)) {
                registers(i) = TransformerOutput(sampleTransformer)
              }

              SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, 0, transformerSize))
            }

            profiles(i) = generalizeProfiles(totalCount, sampleProfiles)
          }

          case est: EstimatorNode => {
            val size = SparkUtilWrapper.estimateSize(est)
            profiles(i) = Profile(0, 0, size)
            registers(i) = EstimatorOutput(est)
          }

          case transformer: TransformerNode => {
            val size = SparkUtilWrapper.estimateSize(transformer)
            profiles(i) = Profile(0, 0, size)
            registers(i) = TransformerOutput(transformer)
          }
        }
      } else {
        // Execute instructions that don't need to be profiled
        instruction match {
          case SourceNode(rdd) => {
            val npp = WorkflowUtils.numPerPartition(rdd)
            numPerPartitionPerNode(i) = npp

            val totalCount = npp.values.map(_.toLong).sum

            val partitionScale = sortedScales.max

            // Calculate the necessary number of items per partition to maintain the same partition distribution,
            // while only having scale items instead of totalCount items.
            // Can't use mapValues because that isn't serializable
            val scale = partitionScale * npp.size
            val scaledNumPerPartition = npp.toSeq
              .map(x => (x._1, math.round((scale.toDouble / totalCount) * x._2).toInt)).toMap

            // Construct a sample containing only scale items, but w/ the same relative partition distribution
            val sample = rdd.mapPartitionsWithIndex {
              case (pid, partition) => partition.take(scaledNumPerPartition(pid))
            }

            registers(i) = RDDOutput(sample)
          }

          case _ => {
            registers(i) = instruction.execute(instruction.getDependencies.map(registers))
          }
        }
      }
    }

    // Unpersist anything that may still be cached
    registers.foreach {
      case RDDOutput(rdd) => rdd.unpersist()
      case _ => Unit
    }

    profiles.toMap
  }


  /**
   * Estimates the total runtime of a pipeline given the cached set of instructions
   */
  def estimateCachedRunTime(
    instructions: Seq[Instruction],
    immediateChildrenByInstruction: Map[Int, Seq[Int]],
    cached: Set[Int],
    profiles: Map[Int, Profile]
  ): Double = {
    val nodeWeights = getNodeWeights(instructions)
    val runs = getRuns(instructions, immediateChildrenByInstruction, cached, nodeWeights)
    val localWork = instructions.indices.map(i => profiles.getOrElse(i, Profile(0, 0, 0)).ns.toDouble).toArray

    instructions.indices.map(i => {
      val executions = if (cached(i)) 1 else runs(i)
      localWork(i) * executions
    }).sum
  }

  /**
   * Given a seq of instructions and a set of indices to cache - return an instruction seq with the indices cached.
   */
  def makeCachedPipeline(pipe: Seq[Instruction], cached: Set[Int]): Seq[Instruction] = {
    // Find the indexes of the new caching nodes. We only cache instructions that produce RDDs (and aren't cachers)
    val dataOutputtingInstructions = pipe.zipWithIndex.filter {
      case (TransformerApplyNode(t, _), _) => !pipe(t).isInstanceOf[Cacher[_]]
      case (SourceNode(_), _) => true
      case _ => false
    }.map(_._2).toSet

    val toCache = cached.intersect(dataOutputtingInstructions)

    pipe.indices.foldLeft (
      (Seq[Instruction](), pipe.indices.zipWithIndex.toMap + (Pipeline.SOURCE -> Pipeline.SOURCE))
    ) {
      case ((newPipe, oldToNewIndexMap), i) if toCache.contains(i) =>
        (newPipe ++ Seq(
          pipe(i).mapDependencies(oldToNewIndexMap),
          new Cacher,
          TransformerApplyNode(oldToNewIndexMap(i) + 1, Seq(oldToNewIndexMap(i)))
        ),
          oldToNewIndexMap.map {
            case (key, value) => if (key >= i) (key, value + 2) else (key, value)
          })

      case ((newPipe, oldToNewIndexMap), i) =>
        (newPipe :+ pipe(i).mapDependencies(oldToNewIndexMap), oldToNewIndexMap)
    }._1
  }

  def aggressiveCache(instructions: Seq[Instruction]): Seq[Instruction] = {
    val immediateChildren = getImmediateChildrenByInstruction(instructions)
    val nodeWeights = getNodeWeights(instructions)

    val childrenOfSource = WorkflowUtils.getChildren(Pipeline.SOURCE, instructions)

    // Cache any node whose direct output is used more than once while training
    val instructionsToCache = instructions.indices.filter {
      i => immediateChildren(i).filterNot(childrenOfSource).map(nodeWeights).sum > 1
    }.toSet

    makeCachedPipeline(instructions, instructionsToCache)
  }

  def cacheMem(caches: Set[Int], profiles: Map[Int, Profile]): Long = {
    // Must do a toSeq here otherwise the map may merge equal memories
    caches.toSeq.map(i => profiles.getOrElse(i, Profile(0, 0, 0)).rddMem).sum
  }

  /**
   * Returns true iff there is still an uncached node whose output is used > once, that would fit in memory
   * if cached
   */
  def stillRoom(caches: Set[Int], runs: Array[Int], profiles: Map[Int, Profile], spaceLeft: Long): Boolean = {
    runs.zipWithIndex.exists { case (curRuns, index) =>
      (curRuns > 1) &&
        (!caches.contains(index)) &&
        (profiles.getOrElse(index, Profile(0, 0, 0)).rddMem < spaceLeft)
    }
  }

  def selectNext(
    pipe: Seq[Instruction],
    profiles: Map[Int, Profile],
    immediateChildrenByInstruction: Map[Int, Seq[Int]],
    cached: Set[Int],
    runs: Array[Int],
    spaceLeft: Long
  ): Int = {
    //Get the uncached node which fits that maximizes savings in runtime.
    pipe.indices.filter(i =>
      !cached(i) &&
        profiles.getOrElse(i, Profile(0, 0, 0)).rddMem < spaceLeft &&
        runs(i) > 1)
      .minBy(i => estimateCachedRunTime(pipe, immediateChildrenByInstruction, cached + i, profiles))
  }

  def greedyCache(
    instructions: Seq[Instruction],
    profiles: Map[Int, Profile],
    maxMem: Option[Long]
  ): Seq[Instruction] = {
    val nodeWeights = getNodeWeights(instructions)
    val immediateChildrenByInstruction = getImmediateChildrenByInstruction(instructions)

    var cached = initCacheSet(instructions)
    val memBudget = maxMem.getOrElse(instructions.collectFirst {
      case SourceNode(rdd) =>
        val sc = rdd.sparkContext
        // Calculate memory budget, ignoring driver unless in local mode
        val workers = if (sc.getExecutorStorageStatus.size == 1) {
          sc.getExecutorStorageStatus
        } else {
          sc.getExecutorStorageStatus.filter(w => !w.blockManagerId.isDriver)
        }

        val totalMem = workers.map(_.memRemaining.toDouble).sum
        totalMem * 0.75
    }.getOrElse(0.0).toLong)

    var usedMem = cacheMem(cached, profiles)
    var runs = getRuns(instructions, immediateChildrenByInstruction, cached, nodeWeights)
    while (usedMem < memBudget && stillRoom(cached, runs, profiles, memBudget - usedMem)) {
      cached = cached + selectNext(
        instructions,
        profiles,
        immediateChildrenByInstruction,
        cached,
        runs,
        memBudget - usedMem)
      runs = getRuns(instructions, immediateChildrenByInstruction, cached, nodeWeights)
      usedMem = cacheMem(cached, profiles)
    }

    makeCachedPipeline(instructions, cached -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions))
  }

  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)

    WorkflowUtils.instructionsToPipeline(cachingMode match {
      case AggressiveCache => aggressiveCache(instructions)
      case GreedyCache(maxMem, profileScales, numProfileTrials) => {
        logInfo("Starting pipeline profile")
        val profiles = profileInstructions(instructions, profileScales, numProfileTrials)
        logInfo("Finished pipeline profile")

        val instructionsWithProfiles = instructions.zipWithIndex.map {
          case (instruction, i) => (i, instruction, profiles.get(i)).toString()
        }.mkString(",\n")

        logInfo(instructionsWithProfiles)

        logInfo("Starting cache selection")
        val cachedInstructions = greedyCache(instructions, profiles, maxMem)
        logInfo("Finished cache selection")
        cachedInstructions
      }
    })
  }
}

object AutoCacheRule {
  sealed trait CachingStrategy
  /**
   * AggressiveCache assumes sufficient memory for caching everything that gets reused, and caches
   * all of it using an LRU policy.
   * (cache at every node with > 1 direct output dependency edge during training)
   */
  case object AggressiveCache extends CachingStrategy

  /**
   * Greedy caching strategy given a memory budget.
   *
   * If no maxMem is provided, the memory budget defaults to 75% of the remaining memory on the cluster
   * (given whatever RDDs are already cached)
   *
   * @param maxMem The memory budget (bytes)
   * @param partitionScales The scales to sample at (average number of desired data points per partition)
   * @param numProfileTrials The number of profiling samples to take per scale
   */
  case class GreedyCache(
    maxMem: Option[Long] = None,
    partitionScales: Seq[Long] = Seq(2, 4),
    numProfileTrials: Int = 1
  ) extends CachingStrategy
}
