package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import nodes.stats.{StandardScalerModel, StandardScaler}
import nodes.util.{VectorSplitter, Identity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.{MatrixUtils, Stats}
import workflow.{Transformer, LabelEstimator}


/**
 * @param models different kernel models for different lambdas
 * @param blockSize blockSize to split data before applying transformations
 * @param lambdas  lambdas corresponding to the models
 * @param in the training Data
 * @param kernelGen the kernel generator
 * @param nTrain number j training examples
 **/

class KernelBlockModel(
  model: Seq[DenseMatrix[Double]],
  blockSize: Int,
  lambdas: Array[Double],
  kernelGen: KernelGenerator,
  nTrain: Int)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {

  val vectorSplitter = new VectorSplitter(blockSize)
  val numClasses = model(0).cols

  override  def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    apply(vectorSplitter(in))
  }

  def apply(in: Seq[RDD[DenseVector[Double]]]): RDD[DenseVector[Double]] = 
  {
    val out: Seq[RDD[DenseMatrix[Double]]] = in.zipWithIndex.map { (xi: (RDD[DenseVector[Double]], Int)) =>
      val x = xi._1
      val i = xi._2
      val trainSize: Int = nTrain
      val testSize = x.count()
      val numBlocks = math.ceil(trainSize.toDouble/blockSize).toInt

      val modelB = x.context.broadcast(model(i))
      val testMatrix = MatrixUtils.rowsToMatrix(x)


      /* Initially all predictions are 0 */
      var predictions = testMatrix.map(x => DenseMatrix.zeros[Double](x.rows, numClasses))

      for (block <- (0 until numBlocks)) {
        val blockIdxs = (blockSize * block) until (math.min(trainSize, (block + 1) * blockSize))
        val blockIdxsBroadcast =  x.context.broadcast(blockIdxs)
        val testKernelMat:  RDD[DenseMatrix[Double]] = MatrixUtils.rowsToMatrix(kernelGen(x, blockIdxs)).cache()

        val partialPredictions =
        testKernelMat.map {  testKernelBB =>
          val modelBlock = modelB.value(blockIdxsBroadcast.value,::)
          val pp = testKernelBB * modelBlock
          pp
        }

        /* Update predictions  */
        predictions =
        predictions.zip(partialPredictions).map  { case(pred, partialPred) =>
            pred :+ partialPred
        }
      }
      /* Materialize  predictions */
      predictions.count()
      predictions
    }
    val matOut:RDD[DenseMatrix[Double]] = out.reduceLeft((sum, next) => sum.zip(next).map(c => c._1 + c._2))
    matOut.flatMap(x => MatrixUtils.matrixToRowArray(x))
  }

  def apply(in: DenseVector[Double]): DenseVector[Double]  = {
   kernelGen(in)
  }

}




