package nodes.learning

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import breeze.linalg._

import org.apache.spark.rdd.RDD

import utils.{MatrixUtils, Stats}
import workflow.{Transformer, LabelEstimator}

/**
 * Defines a wrapper to access elements of a symmetric distributed 
 * matrix that is generated using a kernel function.
 */
trait KernelMatrix {

  /**
   * Extract specified columns from the kernel matrix. 
   *
   * @param colIdxs the column indexes to extract
   * @return A sub-matrix of size n x idxs.size as an RDD.
   */
  def apply(colIdxs: Seq[Int]): RDD[DenseMatrix[Double]]
}

/**
 * Column-wise block implementation of a kernel matrix.
 * This class uses a kernel transformer to lazily populate the column blocks
 * and caches them optionally
 */
class BlockKernelMatrix[T: ClassTag](
    val kernelGen: KernelTransformer[T],
    val data: RDD[T],
    val cacheKernel: Boolean)
  extends KernelMatrix {

  val cache = HashMap.empty[Seq[Int], RDD[DenseMatrix[Double]]]

  def apply(colIdxs: Seq[Int]): RDD[DenseMatrix[Double]] = {
    if (cache.contains(colIdxs)) {
      cache(colIdxs)
    } else {
      val kBlock = MatrixUtils.rowsToMatrix(kernelGen.computeKernel(data, colIdxs))
      if (cacheKernel) {
        kBlock.cache()
        cache += (colIdxs -> kBlock)
      }
      kBlock
    }
  }
}
