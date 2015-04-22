package nodes

import pipelines._
import breeze.linalg._
import java.util.Random
import pipelines.Pipelines.Transformer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import breeze.stats.distributions._

/** A node that takes in DenseVector[Double] and randomly flips 
  *  the sign of some of the elements */

case class RandomSignNode(val signs: DenseVector[Double])
    extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val signsb = in.context.broadcast(signs)
    in.map(row => row :* signsb.value)
  }
}

object RandomSignNode {
  /* Create a random sign node */
  def create(size: Int): RandomSignNode = {
    val signs = convert(DenseVector.rand(size,Rand.randInt(0,1)), Double)
    signs :+= -1.0
    RandomSignNode(signs)
  }
}
