package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix}
import nodes.stats.{StandardScalerModel, StandardScaler}
import org.apache.spark.rdd.RDD
import nodes.util.{VectorSplitter, Identity}
import utils.{MatrixUtils, Stats, KernelUtils}
import workflow.{Transformer, LabelEstimator}


/**
 * @param models different kernel models for different lambdas
 * @param blockSize blockSize to split data before applying transformations
 * @param lambdas  lambdas corresponding to the models
 * @param trainData the training Data
 * @param kernelGen the kernel generator
 **/

class KernelBlockModel(
  models: RDD[Array[DenseMatrix[Double]]],
  blockSize: Int,
  lambdas: Array[Double],
  trainData: RDD[DenseVector[Double]],
  kernelGen: KernelGenerator[DenseVector[Double]],
  nTrain: Option[Int] = None
)
extends Transformer[DenseVector[Double], Array[DenseVector[Double]]]
{
  override def apply(in: RDD[DenseVector[Double]]): RDD[Array[DenseVector[Double]]] =  {
    val trainSize: Int = nTrain.getOrElse(trainData.count().toInt)
    val testSize = in.count()
    val numBlocks = math.ceil(trainSize.toDouble/blockSize).toInt
    // Only evaluate first model for now
    val model = models.map(_(0))
    val numClasses = model.take(1)(0).cols
    val testMatrix = KernelUtils.rowsToMatrix(in)

    /* Initially all predictions are 0 */
    var predictions = testMatrix.map(x => DenseMatrix.zeros[Double](x.rows, x.cols))

    for (block <- (0 until numBlocks)) {
      val blockIdxs = (blockSize * block) until (math.min(trainSize, (block + 1) * blockSize))
      val testKernelMat:  RDD[DenseMatrix[Double]] = KernelUtils.rowsToMatrix(kernelGen.generateKernelTestBlock(in, trainData, blockIdxs)).cache()

      val testKernelModelZip: RDD[(DenseMatrix[Double], DenseMatrix[Double])] =
        testKernelMat.zip(model)
      /* Compute partial predictions RDD */

      val partialPredictions =
      testKernelModelZip.map {  case (testKernelBB, modelBlock) =>
        testKernelBB * modelBlock
      }

      /* Update predictions  */
      predictions =
      predictions.zip(partialPredictions).map  { case(pred, partialPred) =>
          pred :+ partialPred
      }
    }
    KernelUtils.matrixToRows(predictions).map(Array(_))
  }

  def apply(in: DenseVector[Double]): Array[DenseVector[Double]]  = {
    /*  TODO: Super Hack will fix later  (vaishaal) */
   apply(models.context.parallelize(Array(in))).collect()(0)
  }

}




