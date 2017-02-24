package nodes.learning

import breeze.linalg._
import breeze.stats._
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import workflow.LabelEstimator

/**
 * An Estimator that fits Linear Discriminant Analysis (currently not calculated in a distributed fashion),
 * and returns a transformer that projects into the new space
 *
 * Solves multi-class LDA via Eigenvector decomposition
 *
 * @param numDimensions number of output dimensions to project to
 */
class LinearDiscriminantAnalysis(numDimensions: Int) extends LabelEstimator[DenseVector[Double], DenseVector[Double], Int] {
  /**
   * Currently this method works only on data that fits in local memory.
   * Hard limit of up to ~4B bytes of feature data due to max Java array length
   *
   * Solves multi-class LDA via Eigenvector decomposition
   *
   * "multi-class Linear Discriminant Analysis" or "Multiple Discriminant Analysis" by
   * C. R. Rao in 1948 (The utilization of multiple measurements in problems of biological classification)
   * http://www.jstor.org/discover/10.2307/2983775?uid=3739560&uid=2&uid=4&uid=3739256&sid=21106766791933
   *
   * Python implementation reference at: http://sebastianraschka.com/Articles/2014_python_lda.html
   *
   * @param data to train on.
   * @param labels Input class labels.
   * @return A PipelineNode which can be called on new data.
   */
  override def fit(data: RDD[DenseVector[Double]], labels: RDD[Int]): LinearMapper[DenseVector[Double]] = {
    val sample = labels.zip(data).collect()
    computeLDA(sample)
  }

  def computeLDA(dataAndLabels: Array[(Int, DenseVector[Double])]): LinearMapper[DenseVector[Double]] = {
    val featuresByClass = dataAndLabels.groupBy(_._1).values.map(x => MatrixUtils.rowsToMatrix(x.map(_._2)))
    val meanByClass = featuresByClass.map(f => mean(f(::, *))) // each mean is a row vector, not col

    val sW = featuresByClass.zip(meanByClass).map(f => {
      val featuresMinusMean = f._1(*, ::) - f._2.t // row vector, not column
      featuresMinusMean.t * featuresMinusMean
    }).reduce(_+_)

    val numByClass = featuresByClass.map(_.rows : Double)
    val features = MatrixUtils.rowsToMatrix(dataAndLabels.map(_._2))
    val totalMean = mean(features(::, *)) // A row-vector, not a column-vector

    val sB = meanByClass.zip(numByClass).map {
      case (classMean, classNum) => {
        val m = classMean - totalMean
        (m.t * m) :* classNum
      }
    }.reduce(_+_)

    val eigen = eig((inv(sW): DenseMatrix[Double]) * sB)
    val eigenvectors = (0 until eigen.eigenvectors.cols).map(eigen.eigenvectors(::, _).toDenseMatrix.t)

    val topEigenvectors = eigenvectors.zip(eigen.eigenvalues.toArray).sortBy(x => -math.abs(x._2)).map(_._1).take(numDimensions)
    val W = DenseMatrix.horzcat(topEigenvectors:_*)

    new LinearMapper(W)
  }
}

