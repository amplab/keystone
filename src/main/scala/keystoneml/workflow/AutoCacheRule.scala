package keystoneml.workflow

import breeze.linalg.{DenseMatrix, DenseVector, max}
import keystoneml.nodes.util.Cacher
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SparkUtilWrapper
import keystoneml.pipelines.Logging

import keystoneml.workflow.AutoCacheRule.{AggressiveCache, CachingStrategy, GreedyCache}

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
    cached: Set[NodeId],
    nodeWeights: Map[NodeId, Int]
  ): Map[NodeId, Int] = {

    linearization.collect {
      case node: NodeId => node
    }.foldRight(Map[NodeId, Int]()) {
      case (node, runsByNode) => {
        val runs = childrenByNode(node).map {
          case child: SinkId => 1
          case child: NodeId => if (cached.contains(child)) {
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
      // Currently, ExpressionOperators are only inserted for Cached RDDs & fitted Transformers
      case _: ExpressionOperator => true
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

  case class ProfilingState(
    registers: Map[NodeId, Expression],
    numPerPartitionPerNode: Map[NodeId, Map[Int, Int]],
    profiles: Map[NodeId, Profile]
  )

  /**
   * Get profiles of nodes in the pipeline
   *
   * @param graph The pipeline DAG
   * @param linearization A linearization in the nodes of the pipeline DAG
   * @param nodesToProfile The nodes to collect profiling information for
   * @param partitionScales The scales to profile at (expected number of data points per partition)
   * @param numTrials The number of times to profile at each scale
   * @return
   */
  def profileNodes(
    graph: Graph,
    linearization: Seq[GraphId],
    nodesToProfile: Set[NodeId],
    partitionScales: Seq[Long],
    numTrials: Int
  ): Map[NodeId, Profile] = {

    // We need to execute all instructions that the nodes we need to profile depend on.
    val nodesToExecute = nodesToProfile.foldLeft(Set[NodeOrSourceId]()) {
      case (executeSet, node) => executeSet ++ AnalysisUtils.getAncestors(graph, node) + node
    }.map {
      case node: NodeId => node
      case _ => throw new IllegalArgumentException("May not profile nodes dependent on sources!")
    }

    val sortedScales = partitionScales.sorted
    val initProfiling = ProfilingState(Map(), Map(), Map())

    def profileTransformer(
      state: ProfilingState,
      node: NodeId,
      transformer: TransformerOperator,
      deps: Seq[NodeId]
    ): ProfilingState = {
      val npp = state.numPerPartitionPerNode(deps.head)
      val totalCount = npp.values.map(_.toLong).sum

      if (nodesToProfile.contains(node) && state.registers(deps.head).isInstanceOf[DatasetExpression]) {
        // Access & cache the rdd inputs
        val inputs = deps.map(state.registers).collect {
          case dataset: DatasetExpression => dataset.get.cache()
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
          }).map(rdd => new DatasetExpression(rdd))

          // Profile sample timing
          val start = System.nanoTime()
          // Construct and cache a sample
          val sample = transformer.batchTransform(sampledInputs).cache()
          sample.count()
          val duration = System.nanoTime() - start

          // Profile sample memory
          val rddSize = sample.context.getRDDStorageInfo.filter(_.id == sample.id).map(_.memSize).head

          (sample, SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, rddSize, 0)))
        }

        // Unpersist all samples that won't be reused, save the final and largest sample
        sampleProfiles.map(_._1).dropRight(1).foreach(_.unpersist())
        val newProfile = generalizeProfiles(totalCount, sampleProfiles.map(_._2))
        val newRegister = new DatasetExpression(sampleProfiles.last._1)

        state.copy(
          registers = state.registers + (node -> newRegister),
          numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp),
          profiles = state.profiles + (node -> newProfile))

      } else if (nodesToProfile.contains(node) && state.registers(deps.head).isInstanceOf[DatumExpression]) {
        val inputs = deps.map(state.registers).collect {
          case datum: DatumExpression => datum
        }

        // Profile sample timing
        val start = System.nanoTime()
        val datum = transformer.singleTransform(inputs)
        val duration = System.nanoTime() - start

        // Profile datum size memory
        val datumSize = datum match {
          case obj: AnyRef =>
            SparkUtilWrapper.estimateSize(obj)
          case _ => 0
        }

        val profile = Profile(duration, 0, datumSize)
        val newRegister = new DatumExpression(datum)

        state.copy(
          registers = state.registers + (node -> newRegister),
          numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp),
          profiles = state.profiles + (node -> profile))

      } else {
        val out = transformer.execute(deps.map(state.registers))
        state.copy(
          registers = state.registers + (node -> out),
          numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp))
      }
    }

    def profileDataset(state: ProfilingState, node: NodeId, rdd: RDD[_]): ProfilingState = {
      val npp = WorkflowUtils.numPerPartition(rdd)

      val totalCount = npp.values.map(_.toLong).sum

      if (nodesToProfile.contains(node)) {
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

          (sample, SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, rddSize, 0)))
        }

        // Unpersist all samples that won't be reused, save the final and largest sample
        sampleProfiles.map(_._1).dropRight(1).foreach(_.unpersist())
        val newProfile = generalizeProfiles(totalCount, sampleProfiles.map(_._2))
        val newRegister = new DatasetExpression(sampleProfiles.last._1)

        state.copy(
          registers = state.registers + (node -> newRegister),
          numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp),
          profiles = state.profiles + (node -> newProfile))
      } else {
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

        val newRegisters = state.registers + (node -> new DatasetExpression(sample))
        state.copy(
          registers = newRegisters,
          numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp))
      }
    }

    val profiling = linearization.collect {
      case node: NodeId if nodesToExecute.contains(node) => node
    }.foldLeft(initProfiling) { case (state, node) => {
      graph.getOperator(node) match {
        case DatasetOperator(rdd) => {
          profileDataset(state, node, rdd)
        }
        case DatumOperator(datum) => {
          if (nodesToProfile.contains(node)) {
            // A DatumOperator doesn't output an RDD
            val newNumPerPartitionPerNode = state.numPerPartitionPerNode + (node -> Map[Int, Int]())
            val newRegisters = state.registers + (node -> new DatumExpression(datum))

            // Profile datum size memory
            val datumSize = datum match {
              case obj: AnyRef =>
                SparkUtilWrapper.estimateSize(obj)
              case _ => 0
            }
            val profile = Profile(0, 0, datumSize)

            state.copy(
              registers = newRegisters,
              numPerPartitionPerNode = newNumPerPartitionPerNode,
              profiles = state.profiles + (node -> profile))

          } else {
            // A DatumOperator doesn't output an RDD
            val newNumPerPartitionPerNode = state.numPerPartitionPerNode + (node -> Map[Int, Int]())
            val newRegisters = state.registers + (node -> new DatumExpression(datum))

            state.copy(registers = newRegisters, numPerPartitionPerNode = newNumPerPartitionPerNode)
          }
        }
        case transformer: TransformerOperator => {
          val deps = graph.getDependencies(node).map(_.asInstanceOf[NodeId])

          profileTransformer(state, node, transformer, deps)
        }
        case op: DelegatingOperator => {
          val deps = graph.getDependencies(node).map(_.asInstanceOf[NodeId])
          val transformer = state.registers(deps.head).asInstanceOf[TransformerExpression].get

          profileTransformer(state, node, transformer, deps.tail)
        }
        case estimator: EstimatorOperator => {
          val deps = graph.getDependencies(node).map(_.asInstanceOf[NodeId])
          val npp = state.numPerPartitionPerNode(deps.head)
          val totalCount = npp.values.map(_.toLong).sum

          if (nodesToProfile.contains(node)) {
            // Access & cache the rdd inputs
            val inputs = deps.map(state.registers).collect {
              case dataset: DatasetExpression => dataset.get.cache()
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
              }).map(rdd => new DatasetExpression(rdd))

              // Profile sample timing
              val start = System.nanoTime()
              // fit the estimator
              val sampleTransformer = estimator.fitRDDs(sampledInputs)
              val duration = System.nanoTime() - start

              // Profile fit transformer memory
              val transformerSize = SparkUtilWrapper.estimateSize(sampleTransformer)

              (sampleTransformer,
                SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, 0, transformerSize)))
            }
            val newProfile = generalizeProfiles(totalCount, sampleProfiles.map(_._2))
            val newRegister = new TransformerExpression(sampleProfiles.last._1)

            state.copy(
              registers = state.registers + (node -> newRegister),
              numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp),
              profiles = state.profiles + (node -> newProfile))

          } else {
            val out = estimator.execute(deps.map(state.registers))

            state.copy(
              registers = state.registers + (node -> out),
              numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> npp))
          }
        }
        case op: ExpressionOperator => {
          op.execute(Seq()) match {
            // A dataset, treat like a DatasetOperator:
            case exp: DatasetExpression => {
              val rdd = exp.get
              profileDataset(state, node, rdd)
            }
            // Not a dataset:
            case exp => {
              if (nodesToProfile.contains(node)) {
                val expSize = exp.get match {
                  case obj: AnyRef =>
                    SparkUtilWrapper.estimateSize(obj)
                  case _ => 0
                }

                val profile = Profile(0, 0, expSize)
                state.copy(
                  registers = state.registers + (node -> exp),
                  numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> Map()),
                  profiles = state.profiles + (node -> profile))
              } else {
                state.copy(
                  registers = state.registers + (node -> exp),
                  numPerPartitionPerNode = state.numPerPartitionPerNode + (node -> Map()))
              }
            }
          }
        }
      }
    }}

    // Unpersist anything that may still be cached
    profiling.registers.values.foreach {
      case dataset: DatasetExpression => dataset.get.unpersist()
      case _ => Unit
    }

    profiling.profiles
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
  def addCachesToPipeline(pipe: Graph, cachesToAdd: Set[NodeId]): Graph = {
    cachesToAdd.foldLeft(pipe) {
      case (curGraph, nodeToCache) => {
        val (newGraph, cacheNode) = curGraph.addNode(Cacher(), Seq())
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
    val descendantsOfSources = getDescendantsOfSources(graph)

    // Cache any node whose direct output is used more than once and isn't already cached
    val instructionsToCache = graph.nodes.filter {
      node => (childrenByNode(node).map {
        case child: NodeId => nodeWeights(child)
        case _ => 1
      }.sum > 1) && (!cached(node)) && (!descendantsOfSources(node))
    }

    addCachesToPipeline(graph, instructionsToCache)
  }

  def cachedMem(cached: Set[NodeId], profiles: Map[NodeId, Profile]): Long = {
    // Must do a toSeq here otherwise the map may merge equal memories
    cached.toSeq.map(i => profiles.getOrElse(i, Profile(0, 0, 0)).rddMem).sum
  }

  /**
   * Returns true iff there is still an uncached node whose output is used > once, that would fit in memory
   * if cached
   */
  def stillRoom(
    cached: Set[NodeId],
    runs: Map[NodeId, Int],
    profiles: Map[NodeId, Profile],
    spaceLeft: Long
  ): Boolean = {
    runs.exists { case (node, curRuns) =>
      (curRuns > 1) &&
        (!cached(node)) &&
        (profiles.getOrElse(node, Profile(0, 0, 0)).rddMem < spaceLeft)
    }
  }

  def selectNext(
    graph: Graph,
    linearization: Seq[GraphId],
    profiles: Map[NodeId, Profile],
    childrenByNode: Map[NodeId, Seq[GraphId]],
    cached: Set[NodeId],
    runs: Map[NodeId, Int],
    spaceLeft: Long
  ): NodeId = {
    //Get the uncached node which fits that maximizes savings in runtime.
    graph.nodes.filter(node =>
      !cached(node) &&
        profiles.getOrElse(node, Profile(0, 0, 0)).rddMem < spaceLeft &&
        runs(node) > 1)
      .minBy(i => estimateCachedRunTime(graph, linearization, childrenByNode, cached + i, profiles))
  }

  def greedyCache(
    graph: Graph,
    profiles: Map[NodeId, Profile],
    maxMem: Option[Long]
  ): Graph = {
    val cached = initCacheSet(graph)
    val nodeWeights = getNodeWeights(graph)
    val childrenByNode = getChildrenForAllNodes(graph)
    val linearization = AnalysisUtils.linearize(graph)
    var runs = getRuns(linearization, childrenByNode, cached, nodeWeights)
    val descendantsOfSources = getDescendantsOfSources(graph)

    var toCache = Set[NodeId]()
    val memBudget = maxMem.getOrElse(graph.operators.values.collectFirst {
      // Collect the sparkContext from any DatasetOperators, and find what is already cached
      case DatasetOperator(rdd) =>
        val sc = rdd.sparkContext
        // Calculate memory budget, ignoring driver unless in local mode
        val workers = if (sc.getExecutorStorageStatus.length == 1) {
          sc.getExecutorStorageStatus
        } else {
          sc.getExecutorStorageStatus.filter(w => !w.blockManagerId.isDriver)
        }

        val totalMem = workers.map(_.memRemaining.toDouble).sum
        totalMem * 0.75
    }.getOrElse(0.0).toLong)

    var usedMem = cachedMem(cached, profiles)
    while (usedMem < memBudget && stillRoom(cached ++ toCache, runs, profiles, memBudget - usedMem)) {
      toCache = toCache + selectNext(
        graph,
        linearization,
        profiles,
        childrenByNode,
        cached ++ toCache,
        runs,
        memBudget - usedMem)
      runs = getRuns(linearization, childrenByNode, cached ++ toCache, nodeWeights)
      usedMem = cachedMem(cached ++ toCache, profiles)
    }

    addCachesToPipeline(graph, toCache -- descendantsOfSources)
  }

  override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix]) = {
    cachingMode match {
      case AggressiveCache => (aggressiveCache(plan), prefixes)
      case GreedyCache(maxMem, profileScales, numProfileTrials) => {
        logInfo("Starting pipeline profile")
        val cached = initCacheSet(plan)
        val nodeWeights = getNodeWeights(plan)
        val childrenByNode = getChildrenForAllNodes(plan)
        val linearization = AnalysisUtils.linearize(plan)
        val runs = getRuns(linearization, childrenByNode, cached, nodeWeights)
        val descendantsOfSources = getDescendantsOfSources(plan)

        // We have the possibility of caching all uncached nodes accessed more than once,
        // That don't depend on the sources.
        // TODO: MUST profile all Cacher nodes!!! (to get cachedMem to be correct)
        val nodesToProfile = plan.nodes
          .filter(i => (!cached(i)) && (runs(i) > 1)) -- descendantsOfSources

        val profiles = profileNodes(plan, linearization, nodesToProfile, profileScales, numProfileTrials)
        logInfo("Finished pipeline profile")

        val instructionsWithProfiles = plan.nodes.map {
          node => (node, plan.getOperator(node), plan.getDependencies(node), profiles.get(node)).toString()
        }.mkString(",\n")

        logInfo(instructionsWithProfiles)

        logInfo("Starting cache selection")
        val cachedInstructions = greedyCache(plan, profiles, maxMem)
        logInfo("Finished cache selection")
        (cachedInstructions, prefixes)
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
