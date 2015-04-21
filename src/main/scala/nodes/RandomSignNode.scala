package nodes

import pipelines._
import breeze.linalg._
import java.util.Random
import pipelines.Pipelines.PipelineNode
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/** A node that takes in DenseVector[Double] and randomly flips 
  *  the sign of some of the elements */

case class RandomSignNode(val signs: DenseVector[Double])
    extends PipelineNode[RDD[DenseVector[Double]], RDD[DenseVector[Double]]] with Serializable {
  def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val signsb = in.context.broadcast(signs)
    in.map(row => RandomSignNode.hadamardProduct(row, signsb.value))
  }
}

object RandomSignNode {
  /* Create a random sign node */
  def create(size: Int, random: Random): RandomSignNode = {
    val signs = new DenseVector[Double](size)
    for (i <- 0 until size) {
      signs(i) = if (random.nextBoolean()) 1.0 else -1.0
    }
    RandomSignNode(signs)
  }
  /* Take the pointwise product of two Arrays */
  def hadamardProduct(a: DenseVector[Double], b: DenseVector[Double]): DenseVector[Double] = {
    assert(a.size == b.size, "RandomSignNode: Input dimension %d does not match output dimension %d".format(a.size, b.size))
    val size = a.size
    val result = new DenseVector[Double](size)
    var i = 0
    while (i < size) {
      result(i) = a(i)*b(i)
      i += 1
    }
    result
  }
}
