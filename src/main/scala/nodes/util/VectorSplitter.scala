package nodes.util

import breeze.linalg._
import org.apache.spark.rdd.RDD
import pipelines.FunctionNode

/**
 * This transformer splits the input vector into a number of blocks.
 */
class VectorSplitter[T <: Vector[Double]](
    blockSize: Int,
    numFeaturesOpt: Option[Int] = None) 
  extends FunctionNode[RDD[T], Seq[RDD[DenseVector[Double]]]] {

  override def apply(in: RDD[T]): Seq[RDD[DenseVector[Double]]] = {
    val numFeatures = numFeaturesOpt.getOrElse(in.first.length)
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      in.map { vec =>
        // Construct the data slice
        val start = blockNum * blockSize
        val end = math.min(numFeatures, (blockNum + 1) * blockSize)
        val slice = Array.tabulate(end - start)(i => vec(i + start))
        DenseVector(slice)
      }
    }
  }

  def splitVector(in: Vector[Double]): Seq[DenseVector[Double]] = {
    val numFeatures = numFeaturesOpt.getOrElse(in.length)
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      // Construct the data slice
      val start = blockNum * blockSize
      val end = math.min(numFeatures, (blockNum + 1) * blockSize)
      val slice = Array.tabulate(end - start)(i => in(i + start))
      DenseVector(slice)
    }
  }
}
