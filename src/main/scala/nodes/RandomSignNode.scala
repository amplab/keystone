package nodes

import breeze.linalg._
import breeze.stats.distributions._
import org.apache.spark.rdd.RDD
import pipelines.Transformer

/** A node that takes in DenseVector[Double] and randomly flips 
  *  the sign of some of the elements */

case class RandomSignNode(signs: DenseVector[Double])
    extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val signsb = in.context.broadcast(signs)
    in.map(row => row :* signsb.value)
  }

  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    in :* signs
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
