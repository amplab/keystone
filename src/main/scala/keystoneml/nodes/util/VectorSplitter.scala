package keystoneml.nodes.util

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import keystoneml.pipelines.FunctionNode

/**
 * This transformer splits the input vector into a number of blocks.
 */
class VectorSplitter(
    blockSize: Int,
    numFeaturesOpt: Option[Int] = None) 
  extends FunctionNode[RDD[DenseVector[Double]], Seq[RDD[DenseVector[Double]]]] {

  override def apply(in: RDD[DenseVector[Double]]): Seq[RDD[DenseVector[Double]]] = {
    val numFeatures = numFeaturesOpt.getOrElse(in.first.length)
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      in.map { vec =>
        // Expliclity call toArray as breeze's slice is lazy
        val end = math.min(numFeatures, (blockNum + 1) * blockSize)
        DenseVector(vec.slice(blockNum * blockSize, end).toArray)
      }
    }
  }

  def splitVector(in: DenseVector[Double]): Seq[DenseVector[Double]] = {
    val numFeatures = numFeaturesOpt.getOrElse(in.length)
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      // Expliclity call toArray as breeze's slice is lazy
      val end = math.min(numFeatures, (blockNum + 1) * blockSize)
      DenseVector(in.slice(blockNum * blockSize, end).toArray)
    }
  }
}
