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
   * NOTE: This returns a *cached* RDD and unpersist should
   * be called at the end of a block.
   *
   * @param colIdxs the column indexes to extract
   * @return A sub-matrix of size n x idxs.size as an RDD.
   */
  def apply(colIdxs: Seq[Int]): RDD[DenseMatrix[Double]]

  /**
   * Extract a diagonal block from the kernel matrix.
   *
   * @param idxs the column, row indexes to extract
   * @return A local matrix of size idxs.size x idxs.size
   */
  def diagBlock(idxs: Seq[Int]): DenseMatrix[Double]

  /**
   * Clean up resources associated with a kernel block.
   *
   * @param colIdxs column indexes corresponding to the block.
   */
  def unpersist(colIdxs: Seq[Int]): Unit
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

  val colBlockCache = HashMap.empty[Seq[Int], RDD[DenseMatrix[Double]]]
  val diagBlockCache = HashMap.empty[Seq[Int], DenseMatrix[Double]]

  def apply(colIdxs: Seq[Int]): RDD[DenseMatrix[Double]] = {
    if (colBlockCache.contains(colIdxs)) {
      colBlockCache(colIdxs)
    } else {
      val (kBlock, diagBlock) = kernelGen.computeKernel(data, colIdxs)
      if (cacheKernel) {
        colBlockCache += (colIdxs -> kBlock)
        diagBlockCache += (colIdxs -> diagBlock)
      }
      kBlock
    }
  }

  def unpersist(colIdxs: Seq[Int]): Unit = {
    if (colBlockCache.contains(colIdxs) && !cacheKernel) {
      colBlockCache(colIdxs).unpersist(true)
    }
  }

  def diagBlock(idxs: Seq[Int]): DenseMatrix[Double] = {
    if (!diagBlockCache.contains(idxs)) {
      val (kBlock, diagBlock) = kernelGen.computeKernel(data, idxs)
      if (cacheKernel) {
        colBlockCache += (idxs -> kBlock)
        diagBlockCache += (idxs -> diagBlock)
      }
      diagBlock
    } else {
      diagBlockCache(idxs)
    }
  }
}
