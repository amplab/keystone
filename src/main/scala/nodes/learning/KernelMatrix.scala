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

  /**
   * Extract specified rows and columns from the kernel matrix.
   *
   * @param rowIdxs the row indexes to extract
   * @param colIdxs the column indexes to extract
   * @return A sub-matrix of size rowIdxs.size x colIdxs.size as an RDD.
   */
  def apply(rowIdxs: Seq[Int], colIdxs: Seq[Int]): RDD[DenseMatrix[Double]]
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

  val cache = HashMap.empty[(Seq[Int], Seq[Int]), RDD[DenseMatrix[Double]]]

  def apply(colIdxs: Seq[Int]): RDD[DenseMatrix[Double]] = {
    if (cache.contains((Seq.empty[Int], colIdxs))) {
      cache((Seq.empty[Int], colIdxs))
    } else {
      val kBlock = MatrixUtils.rowsToMatrix(kernelGen.computeKernel(data, colIdxs))
      if (cacheKernel) {
        kBlock.cache()
        cache += ((Seq.empty[Int], colIdxs) -> kBlock)
      }
      kBlock
    }
  }

  def apply(rowIdxs: Seq[Int], colIdxs: Seq[Int]): RDD[DenseMatrix[Double]] = {
    // Check if the block we are looking for is in cache 
    if (cache.contains((rowIdxs, colIdxs))) {
      cache((rowIdxs, colIdxs))
    } else {
      // TODO: We have a wasteful conversion from vector -> matrix back to vector
      // happening here.
      val kBlock =
        if (cache.contains((Seq.empty[Int], colIdxs))) {
          cache((Seq.empty[Int], colIdxs)).flatMap(MatrixUtils.matrixToRowArray(_))
        } else {
          kernelGen.computeKernel(data, colIdxs)
        }

      val kBlockBlock =
        MatrixUtils.rowsToMatrix(kBlock.zipWithIndex.filter { case (vec, idx) =>
          rowIdxs.contains(idx)
        }.map { x => 
          x._1
        })

      if (cacheKernel) {
        kBlockBlock.cache()
        cache += ((rowIdxs, colIdxs) -> kBlockBlock)
      }
      kBlockBlock
    }
  }
}
