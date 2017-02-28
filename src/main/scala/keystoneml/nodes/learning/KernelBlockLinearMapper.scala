package keystoneml.nodes.learning

import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import breeze.linalg._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import keystoneml.nodes.stats.{StandardScalerModel, StandardScaler}
import keystoneml.nodes.util.{VectorSplitter, Identity}

import keystoneml.utils.{MatrixUtils, Stats}
import keystoneml.workflow.{Transformer, LabelEstimator}

/**
 * Transformer that applies a kernel model to an input.
 * Different from block linear mapper in that this class also
 * applies the kernel function to the input before computing the predictions.
 *
 * @param xs The chunks of the matrix representing the model
 * @param blockSize blockSize to split data before applying transformations
 * @param kernelTransformer the kernel generator
 * @param nTrain number of training examples
 * @param blocksBeforeCheckpoint frequency at which intermediate data should be checkpointed
 */
class KernelBlockLinearMapper[T: ClassTag](
    val model: Seq[DenseMatrix[Double]],
    blockSize: Int,
    kernelTransformer: KernelTransformer[T],
    nTrain: Long,
    blocksBeforeCheckpoint: Int = 25)
  extends Transformer[T, DenseVector[Double]] {

  val numClasses = model(0).cols
  val numBlocks = model.size

  override def apply(in: RDD[T]): RDD[DenseVector[Double]] = {
    val testKernelMat = kernelTransformer(in)
    // Initially all predictions are 0
    var predictions = in.mapPartitions { iter =>
      if (iter.hasNext) {
        val out = DenseMatrix.zeros[Double](iter.size, numClasses)
        Iterator.single(out)
      } else {
        Iterator.empty
      }
    }.cache()

    val modelBCs = new ListBuffer[Broadcast[DenseMatrix[Double]]]

    (0 until numBlocks).foreach { block =>
      val blockIdxs = (blockSize * block) until (math.min(nTrain.toInt, (block + 1) * blockSize))
      val testKernelBlock = testKernelMat(blockIdxs.toSeq)
      val modelBlockBC = in.context.broadcast(model(block))
      modelBCs += modelBlockBC

      // Update predictions
      var predictionsNew = predictions.zip(testKernelBlock).map { case(pred, testKernelBB) =>
        pred :+ (testKernelBB * modelBlockBC.value)
      }

      predictionsNew.cache()
      predictionsNew.count()
      predictions.unpersist(true)

      testKernelMat.unpersist(blockIdxs.toSeq)
      modelBlockBC.unpersist(true)

      // If we are checkpointing update our cache
      if (in.context.getCheckpointDir.isDefined &&
          block % blocksBeforeCheckpoint == (blocksBeforeCheckpoint - 1)) {
        predictionsNew = MatrixUtils.truncateLineage(predictionsNew, false)
      }
      predictions = predictionsNew
    }
    predictions.flatMap(x => MatrixUtils.matrixToRowArray(x))
  }

  def apply(in: T): DenseVector[Double]  = {
    val testKernelRow = kernelTransformer(in)
    val predictions = DenseVector.zeros[Double](numClasses)
    (0 until numBlocks).foreach { block =>
      val blockIdxs = (blockSize * block) until (math.min(nTrain.toInt, (block + 1) * blockSize))
      predictions += (testKernelRow(blockIdxs) * model(block)).toDenseVector
    }
    predictions
  }
}
