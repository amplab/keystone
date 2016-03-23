package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import nodes.stats.{StandardScalerModel, StandardScaler}
import nodes.util.{VectorSplitter, Identity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import utils.{MatrixUtils, Stats}
import workflow.{Transformer, LabelEstimator}

/* Defines a distributed KernelMatrix
 * Note this is a lazy matrix, 
 */

trait KernelMatrix {

  /* Return a n x idxs.size sub matrix (every entry in RDD is a row)
   * @param idxs the column indexes of your kernel matrix you want
   * */
  def apply(idxs: Seq[Int]):RDD[DenseMatrix[Double]]

  def apply(idxs1: Seq[Int], idxs2: Seq[Int]): RDD[DenseMatrix[Double]]

}

class BlockedKernelMatrix (kernelGen: KernelGenerator, data: RDD[DenseVector[Double]],  train: Boolean, cacheKernel: Boolean) extends KernelMatrix
{
  val cache = HashMap.empty[(Seq[Int], Seq[Int]),RDD[DenseMatrix[Double]]]

  def apply(idxs: Seq[Int]):RDD[DenseMatrix[Double]] = {
    val kBlock =
    if (cache contains ((idxs, Seq.empty[Int]))) {
      cache((idxs, Seq.empty[Int]))
    } else {
    val kBlock = toMatrix(kernelGen(data, idxs, train))
      if (cacheKernel) {
        kBlock.cache()
        cache += ((idxs, Seq.empty[Int])  -> kBlock)
      }
    kBlock
    }
    kBlock
  }

  def apply(idxs1: Seq[Int], idxs2: Seq[Int]):RDD[DenseMatrix[Double]] = {
    /* Check if the block we are looking for is in cache */
    val kBlockMatrix =
    if (cache contains (idxs1, idxs2)) {
      cache((idxs1, idxs2))
    } else {
      val kBlock =
      if (cache contains (idxs1, Seq.empty[Int])) {
        cache((idxs1, Seq.empty[Int])).flatMap(MatrixUtils.matrixToRowArray(_))
      } else {
        kernelGen(data, idxs1, false)
      }

      val kBlockBlock =
      toMatrix(kBlock.zipWithIndex.filter{ case (vec, idx) =>
        idxs2.contains(idx)
      }.map(x=>x._1))
      if (cacheKernel) {
        kBlockBlock.cache()
        cache += ((idxs1, idxs2) -> kBlockBlock)
      }
      kBlockBlock
    }
    kBlockMatrix
  }

  def toMatrix(vectors: RDD[DenseVector[Double]]) = {
    vectors.mapPartitions { part =>
      MatrixUtils.rowsToMatrixIter(part)
    }
  }
}
