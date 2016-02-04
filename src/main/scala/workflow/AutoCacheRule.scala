package workflow

import breeze.linalg.{max, DenseVector, DenseMatrix}
import nodes.util.Cacher
import org.apache.spark.rdd.RDD
import pipelines.Logging

case class Profile(ns: Long, mem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.mem + p.mem)
}

case class SampleProfile(scale: Long, profile: Profile)

class AutoCacheRule(
                     profileScales: Seq[Long] = Seq(500, 1000),
                     numProfileTrials: Int = 2
                   ) extends Rule with Logging {

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
  def getImmediateChildrenByInstruction(instructions: Seq[Instruction]): Map[Int, Set[Int]] = {
    instructions.indices.map(i => (i, WorkflowUtils.getChildren(i, instructions))).toMap
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

  def generalizeProfiles(newScale: Long, sampleProfiles: Seq[SampleProfile]): Profile = {
    def getModel(inp: Iterable[(Long, String, Long)]): Double => Double = {
      val observations = inp.toArray

      //Pack a data matrix with observations
      val X = DenseMatrix.ones[Double](observations.length, 2)
      observations.zipWithIndex.foreach(o => X(o._2, 0) = o._1._1.toDouble)
      val y = DenseVector(observations.map(_._3.toDouble))
      val model = max(X \ y, 0.0)

      //A function to apply the model.
      def res(x: Double): Double = DenseVector(x, 1.0).t * model

      res
    }

    val samples = sampleProfiles.flatMap { case SampleProfile(scale, value) =>
      Array(
        (scale, "memory", value.mem),
        (scale, "time", value.ns)
      )}.groupBy(a => a._2)

    val models = samples.mapValues(getModel)

    Profile(models("time").apply(newScale).toLong, models("memory").apply(newScale).toLong)
  }

  def profileInstructions(
                           instructions: Seq[Instruction],
                           scales: Seq[Long],
                           numTrials: Int
                         ): Map[Int, Profile] = {
    val instructionsToProfile = instructions.indices.toSet -- WorkflowUtils.getChildren(Pipeline.SOURCE, instructions)

    val registers = scala.collection.mutable.Map[Int, Any]()
    val numPerPartitionPerNode = scala.collection.mutable.Map[Int, Map[Int, Int]]()
    val profiles = scala.collection.mutable.Map[Int, Profile]()

    val sortedScales = scales.sorted
    for ((instruction, i) <- instructions.zipWithIndex if instructionsToProfile.contains(i)) {
      instruction match {
        case SourceNode(rdd) =>
          val npp = WorkflowUtils.numPerPartition(rdd)
          numPerPartitionPerNode(i) = npp

          val totalCount = npp.values.map(_.toLong).sum

          val sampleProfiles = for (
            (scale, scaleIndex) <- sortedScales.zipWithIndex;
            trial <- 1 to numTrials
          ) yield {
            // Calculate the necessary number of items per partition to maintain the same partition distribution,
            // while only having scale items instead of totalCount items.
            // Can't use mapValues because that isn't serializable
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
            val memSize = sample.context.getRDDStorageInfo.filter(_.id == sample.id).map(_.memSize).head

            // If this sample was computed using the final and largest scale, add it to the registers
            if ((scaleIndex == (sortedScales.length - 1)) && (trial == numTrials)) {
              registers(i) = sample
            } else {
              sample.unpersist()
            }

            SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, memSize))
          }

          profiles(i) = generalizeProfiles(totalCount, sampleProfiles)

        case transformer: TransformerNode =>
          registers(i) = transformer

        case estimator: EstimatorNode =>
          registers(i) = estimator

        case TransformerApplyNode(tIndex, inputIndices) =>
          // We assume that all input rdds to this transformer have equal, zippable partitioning
          val npp = numPerPartitionPerNode(inputIndices.head)
          numPerPartitionPerNode(i) = npp
          val totalCount = npp.values.map(_.toLong).sum

          val transformer = registers(tIndex).asInstanceOf[TransformerNode]
          val inputs = inputIndices.map(x => registers(x).asInstanceOf[RDD[_]])

          val sampleProfiles = for (
            (scale, scaleIndex) <- sortedScales.zipWithIndex;
            trial <- 1 to numTrials
          ) yield {
            // Calculate the necessary number of items per partition to maintain the same partition distribution,
            // while only having scale items instead of totalCount items.
            // Can't use mapValues because that isn't serializable
            val scaledNumPerPartition = npp.toSeq.map(x => (x._1, ((scale.toDouble / totalCount) * x._2).toInt)).toMap

            // Sample the inputs. Samples containing only scale items, but w/ the same relative partition distribution
            // NOTE: Assumes all inputs have equal, zippable partition counts
            val sampledInputs = inputs.map(_.mapPartitionsWithIndex {
              case (pid, partition) => partition.take(scaledNumPerPartition(pid))
            })
            sampledInputs.foreach(_.count())

            // Profile sample timing
            val start = System.nanoTime()
            // Construct a
            val sample = transformer.transformRDD(sampledInputs).cache()
            sample.count()
            val duration = System.nanoTime() - start

            // Profile sample memory
            val memSize = sample.context.getRDDStorageInfo.filter(_.id == sample.id).map(_.memSize).head

            // If this sample was computed using the final and largest scale, add it to the registers
            if ((scaleIndex == (sortedScales.length - 1)) && (trial == numTrials)) {
              registers(i) = sample
            } else {
              sample.unpersist()
            }

            SampleProfile(scaledNumPerPartition.values.sum, Profile(duration, memSize))
          }

          profiles(i) = generalizeProfiles(totalCount, sampleProfiles)

        case EstimatorFitNode(estIndex, inputIndices) =>
          val estimator = registers(estIndex).asInstanceOf[EstimatorNode]
          val inputs = inputIndices.map(x => registers(x).asInstanceOf[RDD[_]])
          registers(i) = estimator.fitRDDs(inputs)
      }
    }

    profiles.toMap
  }

  def estimateCachedRunTime(instructions: Seq[Instruction], cached: Set[Int], profiles: Map[Int, Profile]): Double = {
    val nodeWeights = getNodeWeights(instructions)
    val runs = getRuns(instructions, cached, nodeWeights)

    val localWork = instructions.indices.map(i => profiles.getOrElse(i, Profile(0, 0)).ns.toDouble).toArray

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
    val filteredCaches = pipe.zipWithIndex.filter {
      case (TransformerApplyNode(_, _), _) => true
      case (SourceNode(_), _) => true
      case _ => false
    }.map(_._2).toSet

    val toCache = cached.intersect(filteredCaches)

    pipe.indices.foldLeft((Seq[Instruction](), pipe.indices.zipWithIndex.toMap)) {
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

  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)

    val profiles = profileInstructions(instructions, profileScales, numProfileTrials)

    null
  }
}
