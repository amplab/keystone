package workflow.graph

import breeze.linalg.{DenseMatrix, DenseVector, max}
import nodes.util.Cacher
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SparkUtilWrapper
import pipelines.Logging
import workflow.AutoCacheRule.{AggressiveCache, CachingStrategy, GreedyCache}

case class Profile(ns: Long, rddMem: Long, driverMem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.rddMem + p.rddMem, this.driverMem + p.driverMem)
}

case class SampleProfile(scale: Long, profile: Profile)

class AutoCacheRule(cachingMode: CachingStrategy) extends Rule with Logging {

  /**
   * Get the operator weights: estimates for how many passes an operator will make over its input dependencies
   */
  def getNodeWeights(graph: Graph): Map[NodeId, Int] = {
    graph.operators.mapValues {
      case op: WeightedOperator => op.weight
      case _ => 1
    }
  }

  /**
   * Get all descendents of all sources in the graph
   */
  def getDescendantsOfSources(graph: Graph): Set[NodeId] = {
    graph.sources.foldLeft(Set[GraphId]()) {
      case (curDescendents, source) => curDescendents ++ AnalysisUtils.getDescendants(graph, source)
    }.collect {
      case node: NodeId => node
    }
  }

  /**
   * Get a map representing the children for each node
   * Note: This doesn't capture how many times each child depended on the instruction
   */
  def getChildrenForAllNodes(graph: Graph): Map[NodeId, Seq[GraphId]] = {
    graph.nodes.map {
      id => (id, AnalysisUtils.getChildren(graph, id).toSeq)
    }.toMap
  }

  /**
   * Get an estimate for how many times the output of each node will be accessed, assuming
   * the given set of nodes have their outputs cached.
   *
   * Note: This assumes all sinks are accessed exactly once!
   */
  def getRuns(
      linearization: Seq[GraphId],
      childrenByNode: Map[NodeId, Seq[GraphId]],
      cache: Set[NodeId],
      nodeWeights: Map[NodeId, Int]
    ): Map[NodeId, Int] = {

    linearization.collect {
      case node: NodeId => node
    }.foldRight(Map[NodeId, Int]()) {
      case (node, runsByNode) => {
        val runs = childrenByNode(node).map {
          case child: SinkId => 1
          case child: NodeId => if (cache.contains(child)) {
            nodeWeights(child)
          } else {
            nodeWeights(child) * runsByNode(child)
          }
          case _: SourceId => throw new IllegalArgumentException("Invalid graph, node has source as child")
        }.sum

        runsByNode + (node -> runs)
      }
    }
  }

  /**
   * Get the initial set of what nodes will have their results effectively cached
   */
  def initCacheSet(graph: Graph): Set[NodeId] = {
    graph.nodes.filter { graph.getOperator(_) match {
      case DatasetOperator(rdd) => rdd.getStorageLevel != StorageLevel.NONE
      case _: DatumOperator => true
      case _: Cacher[_] => true
      case _: TransformerOperator => false
      case _: EstimatorOperator => true
      case _: DelegatingOperator => false
    }}
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
    graph: Graph,
    partitionScales: Seq[Long],
    numTrials: Int
  ): Map[NodeId, Profile] = {
    val cached = initCacheSet(graph)
    val nodeWeights = getNodeWeights(graph)
    val childrenByNode = getChildrenForAllNodes(graph)
    val linearization = AnalysisUtils.linearize(graph)
    val runs = getRuns(linearization, childrenByNode, cached, nodeWeights)
    val descendantsOfSources = getDescendantsOfSources(graph)

    // We have the possibility of caching all uncached nodes accessed more than once,
    // That don't depend on the sources.
    val instructionsToProfile = graph.nodes
      .filter(i => !cached(i))
      .filter(i => runs(i) > 1) -- descendantsOfSources

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
   * Estimates the total runtime of a pipeline given the cached set of nodes
   */
  def estimateCachedRunTime(
    graph: Graph,
    linearization: Seq[GraphId],
    childrenByNode: Map[NodeId, Seq[GraphId]],
    cached: Set[NodeId],
    profiles: Map[NodeId, Profile]
  ): Double = {
    val nodeWeights = getNodeWeights(graph)
    val runs = getRuns(linearization, childrenByNode, cached, nodeWeights)

    graph.nodes.map { node =>
      val executions = if (cached(node)) 1 else runs(node)
      val localWork = profiles.getOrElse(node, Profile(0, 0, 0)).ns.toDouble

      localWork * executions
    }.sum
  }

  /**
   * Given a pipeline DAG and an additional set of nodes to cache - return a DAG with the nodes cached.
   */
  def makeCachedPipeline(pipe: Graph, cachesToAdd: Set[NodeId]): Graph = {
    cachesToAdd.foldLeft(pipe) {
      case (curGraph, nodeToCache) => {
        val (newGraph, cacheNode) = curGraph.addNode(Cacher[_](), Seq())
        newGraph
          .replaceDependency(nodeToCache, cacheNode)
          .setDependencies(cacheNode, Seq(nodeToCache))
      }
    }
  }

  def aggressiveCache(graph: Graph): Graph = {
    val cached = initCacheSet(graph)
    val nodeWeights = getNodeWeights(graph)
    val childrenByNode = getChildrenForAllNodes(graph)
    val linearization = AnalysisUtils.linearize(graph)
    val runs = getRuns(linearization, childrenByNode, cached, nodeWeights)

    val descendantsOfSources = getDescendantsOfSources(graph)

    // Cache any node whose direct output is used more than once and isn't already cached
    val instructionsToCache = graph.nodes.filter {
      node => (runs.getOrElse(node, 0) > 1) && (!cached(node)) && (!descendantsOfSources(node))
    }

    makeCachedPipeline(graph, instructionsToCache)
  }

  def cacheMem(caches: Set[Int], profiles: Map[Int, Profile]): Long = {
    // Must do a toSeq here otherwise the map may merge equal memories
    caches.toSeq.map(i => profiles.getOrElse(i, Profile(0, 0, 0)).rddMem).sum
  }

  /**
   * Returns true iff there is still an uncached node whose output is used > once, that would fit in memory
   * if cached
   */
  def stillRoom(
    caches: Set[NodeId],
    runs: Map[NodeId, Int],
    profiles: Map[NodeId, Profile],
    spaceLeft: Long
  ): Boolean = {
    runs.exists { case (node, curRuns) =>
      (curRuns > 1) &&
        (!caches.contains(node)) &&
        (profiles.getOrElse(node, Profile(0, 0, 0)).rddMem < spaceLeft)
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
    graph: Graph,
    profiles: Map[NodeId, Profile],
    maxMem: Option[Long]
  ): Graph = {
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

  override def apply(plan: Graph): Graph = {
    cachingMode match {
      case AggressiveCache => aggressiveCache(plan)
      case GreedyCache(maxMem, profileScales, numProfileTrials) => {
        logInfo("Starting pipeline profile")
        val profiles = profileInstructions(plan, profileScales, numProfileTrials)
        logInfo("Finished pipeline profile")

        val instructionsWithProfiles = instructions.zipWithIndex.map {
          case (instruction, i) => (i, instruction, profiles.get(i)).toString()
        }.mkString(",\n")

        logInfo(instructionsWithProfiles)

        logInfo("Starting cache selection")
        val cachedInstructions = greedyCache(plan, profiles, maxMem)
        logInfo("Finished cache selection")
        cachedInstructions
      }
    }
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
