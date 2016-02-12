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
  def getRuns(instructions: Seq[Instruction], cache: Set[Int], nodeWeights: Map[Int, Int]): Map[Int, Int] = {
    val immediateChildrenByInstruction = getImmediateChildrenByInstruction(instructions)

    instructions.indices.foldRight(Map[Int, Int]()) { case (i, runsByIndex) =>
      if (immediateChildrenByInstruction(i).isEmpty) {
        runsByIndex + (i -> 1)
      }
      else {
        val runs = immediateChildrenByInstruction(i).map(j => if (cache.contains(j)) {
          nodeWeights(j)
        } else {
          nodeWeights(j) * runsByIndex(j)
        }).sum
        runsByIndex + (i -> runs)
      }
    }
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
    val instructionsToProfile = instructions.indices.toSet -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions)

    val registers = new Array[InstructionOutput](instructions.length)
    val numPerPartitionPerNode = scala.collection.mutable.Map[Int, Map[Int, Int]]()
    val profiles = scala.collection.mutable.Map[Int, Profile]()

    val sortedScales = partitionScales.sorted
    for ((instruction, i) <- instructions.zipWithIndex if instructionsToProfile.contains(i)) {
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
            val scaledNumPerPartition = npp.toSeq.map(x => (x._1, ((scale.toDouble / totalCount) * x._2).toInt)).toMap

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
            val scaledNumPerPartition = npp.toSeq.map(x => (x._1, ((scale.toDouble / totalCount) * x._2).toInt)).toMap

            // Sample the inputs. Samples containing only scale items, but w/ the same relative partition distribution
            // NOTE: Assumes all inputs have equal, zippable partition counts
            val sampledInputs = inputs.map(_.mapPartitionsWithIndex {
              case (pid, partition) => partition.take(scaledNumPerPartition(pid))
            })

            // Profile sample timing
            val start = System.nanoTime()
            // Construct and cache a sample
            val sample = transformer.transformRDD(sampledInputs).cache()
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
            val scaledNumPerPartition = npp.toSeq.map(x => (x._1, ((scale.toDouble / totalCount) * x._2).toInt)).toMap

            // Sample the inputs. Samples containing only scale items, but w/ the same relative partition distribution
            // NOTE: Assumes all inputs have equal, zippable partition counts
            val sampledInputs = inputs.map(_.mapPartitionsWithIndex {
              case (pid, partition) => partition.take(scaledNumPerPartition(pid))
            })

            // Profile sample timing
            val start = System.nanoTime()
            // fit the estimator
            val sampleTransformer = estimator.fitRDDs(sampledInputs)
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
    cached: Set[Int],
    profiles: Map[Int, Profile]
  ): Double = {
    val nodeWeights = getNodeWeights(instructions)
    val runs = getRuns(instructions, cached, nodeWeights)
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
    // Find the indexes of the new caching nodes. We only cache instructions that produce RDDs
    val dataOutputtingInstructions = pipe.zipWithIndex.filter {
      case (TransformerApplyNode(_, _), _) => true
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
  def stillRoom(caches: Set[Int], runs: Map[Int, Int], profiles: Map[Int, Profile], spaceLeft: Long): Boolean = {
    runs.exists { i =>
      (i._2 > 1) &&
        (!caches.contains(i._1)) &&
        (profiles.getOrElse(i._1, Profile(0, 0, 0)).rddMem < spaceLeft)
    }
  }

  def selectNext(
    pipe: Seq[Instruction],
    profiles: Map[Int, Profile],
    cached: Set[Int],
    runs: Map[Int, Int],
    spaceLeft: Long
  ): Int = {
    //Get the uncached node which fits that maximizes savings in runtime.
    pipe.indices.filter(i =>
      !cached(i) &&
        profiles.getOrElse(i, Profile(0, 0, 0)).rddMem < spaceLeft &&
        runs(i) > 1)
      .minBy(i => estimateCachedRunTime(pipe, cached + i, profiles))
  }

  def greedyCache(
    instructions: Seq[Instruction],
    profiles: Map[Int, Profile],
    maxMem: Option[Long]
  ): Seq[Instruction] = {
    val nodeWeights = getNodeWeights(instructions)

    var cached = instructions.indices.filter { instructions(_) match {
      case _: EstimatorNode => true
      case _: TransformerNode =>  true
      case _: EstimatorFitNode => true
      case _: SourceNode => false
      case _: TransformerApplyNode => false
    }}.toSet

    val memBudget = maxMem.getOrElse(instructions.collectFirst {
      case SourceNode(rdd) =>
        val sc = rdd.sparkContext
        val totalMem = sc.getExecutorStorageStatus.map(_.memRemaining.toDouble).sum
        totalMem * 0.75
    }.getOrElse(0.0).toLong)

    var usedMem = cacheMem(cached, profiles)
    var runs = getRuns(instructions, cached, nodeWeights)
    while (usedMem < memBudget && stillRoom(cached, runs, profiles, memBudget - usedMem)) {
      cached = cached + selectNext(instructions, profiles, cached, runs, memBudget - usedMem)
      runs = getRuns(instructions, cached, nodeWeights)
      usedMem = cacheMem(cached, profiles)
    }

    makeCachedPipeline(instructions, cached -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions))
  }

  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)

    WorkflowUtils.instructionsToPipeline(cachingMode match {
      case AggressiveCache => aggressiveCache(instructions)
      case GreedyCache(maxMem, profileScales, numProfileTrials) => {
        val profiles = profileInstructions(instructions, profileScales, numProfileTrials)
        greedyCache(instructions, profiles, maxMem)
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
