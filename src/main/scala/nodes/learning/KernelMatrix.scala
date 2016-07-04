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

class BlockedKernelMatrix(
    val kernelGen: KernelGenerator,
    val data: RDD[DenseVector[Double]],
    val cacheKernel: Boolean)
  extends KernelMatrix
{
  val cache = HashMap.empty[(Seq[Int], Seq[Int]),RDD[DenseMatrix[Double]]]

  def apply(colIdxs: Seq[Int]):RDD[DenseMatrix[Double]] = {
      if (cache contains ((Seq.empty[Int], colIdxs))) {
        cache((Seq.empty[Int], colIdxs))
      } else {
        val kBlock = toMatrix(kernelGen(data, colIdxs))
        if (cacheKernel) {
          kBlock.cache()
          cache += ((Seq.empty[Int], colIdxs)  -> kBlock)
        }
        kBlock
      }
  }

  def apply(rowIdxs: Seq[Int], colIdxs: Seq[Int]):RDD[DenseMatrix[Double]] = {
    /* Check if the block we are looking for is in cache */
     if (cache contains (rowIdxs, colIdxs)) {
       cache((rowIdxs, colIdxs))
     } else {
       val kBlock =
         if (cache contains (Seq.empty[Int], colIdxs)) {
           cache((Seq.empty[Int], colIdxs)).flatMap(MatrixUtils.matrixToRowArray(_))
         } else {
           kernelGen(data, colIdxs)
         }

         val kBlockBlock =
           toMatrix(kBlock.zipWithIndex.filter{ case (vec, idx) =>
             rowIdxs.contains(idx)
           }.map(x=>x._1))
         if (cacheKernel) {
           kBlockBlock.cache()
           cache += ((rowIdxs, colIdxs) -> kBlockBlock)
         }
         kBlockBlock
     }
  }

  def toMatrix(vectors: RDD[DenseVector[Double]]) = {
    vectors.mapPartitions { part =>
      MatrixUtils.rowsToMatrixIter(part)
    }
  }
}
