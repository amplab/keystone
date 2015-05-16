package nodes.misc

import breeze.linalg.DenseVector

import org.apache.spark.rdd.RDD

import pipelines.FunctionNode

/**
 * This transformer splits the input vector into a number of blocks.
 */
class VectorSplitter(blockSize: Int) extends FunctionNode[RDD[DenseVector[Double]], Seq[RDD[DenseVector[Double]]]] {
  override def apply(in: RDD[DenseVector[Double]]): Seq[RDD[DenseVector[Double]]] = {
    val numFeatures = in.first.length
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      in.map { vec =>
        // Expliclity call toArray as breeze's slice is lazy
        DenseVector(vec.slice(blockNum * blockSize, (blockNum + 1) * blockSize).toArray)
      }
    }
  }

  def splitVector(in: DenseVector[Double]): Seq[DenseVector[Double]] = {
    val numFeatures = in.length
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      // Expliclity call toArray as breeze's slice is lazy
      DenseVector(in.slice(blockNum * blockSize, (blockNum + 1) * blockSize).toArray)
    }
  }
}
