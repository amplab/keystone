package workflow

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

case class Profile(ns: Long, mem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.mem + p.mem)
}

class AutoCache(sampleRatio: Double = 0.01) extends Rule {
  private def numPerPartition[T](rdd: RDD[T]): Map[Int, Int] = {
    rdd.mapPartitionsWithIndex {
      case (id, partition) => Iterator.single((id, partition.length))
    }.collect().toMap
  }

  override def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    val instructions = WorkflowUtils.pipelineToInstructions(plan)
    val registers = new ArrayBuffer[Any]()

    val numPerPartitionPerNode = ArrayBuffer[Option[Map[Int, Int]]]()
    val sampledNumPerPartitionPerNode = ArrayBuffer[Option[Map[Int, Int]]]()
    val profile = new ArrayBuffer[Option[Profile]]()

    // FIXME: This whole section isn't implemented yet
    instructions.foreach {
      case SourceNode(rdd) =>
        numPerPartitionPerNode.append(Some(numPerPartition(rdd)))
        val sampledRDD = rdd.sample(false, sampleRatio).cache()

      // FIXME: This whole section isn't implemented yet
      //registers.append(rdd.sample())

      case transformer: TransformerNode =>
        registers.append(transformer)
        numPerPartitionPerNode.append(None)
        sampledNumPerPartitionPerNode.append(None)
        profile.append(None)

      case estimator: EstimatorNode =>
        registers.append(estimator)
        numPerPartitionPerNode.append(None)
        sampledNumPerPartitionPerNode.append(None)
        profile.append(None)

      case TransformerApplyNode(tIndex, inputIndices) =>
        val transformer = registers(tIndex).asInstanceOf[TransformerNode]
        val inputs = inputIndices.map(x => registers(x + 1).asInstanceOf[RDD[_]])
        registers.append(transformer.transformRDD(inputs))
      case EstimatorFitNode(estIndex, inputIndices) =>
        val estimator = registers(estIndex).asInstanceOf[EstimatorNode]
        val inputs = inputIndices.map(x => registers(x + 1).asInstanceOf[RDD[_]])
        registers.append(estimator.fitRDDs(inputs))
    }

    null
  }
}