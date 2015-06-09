package nodes.learning

import breeze.linalg._
import breeze.numerics.{log => bLog, _}
import breeze.stats._
import nodes.stats.MatrixLinearMapperThenMax
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Logging}
import utils.MatrixUtils

case class BensTemplateFiltersEstimator(numTemplateFeatures: Int, zcaWhitener: ZCAWhitener, maxIterations: Int = 100) extends Estimator[DenseMatrix[Double], DenseMatrix[Double]] with Logging {
  require(maxIterations > 0, "maxIterations must be positive")

  def fit(samples: RDD[DenseMatrix[Double]]): MatrixLinearMapperThenMax = {
    fit(DenseMatrix.vertcat(samples.collect():_*))
  }

  def fit(X: DenseMatrix[Double]): MatrixLinearMapperThenMax = {
    val whitenedX = zcaWhitener.apply(X)
    /* normalize the centroids such that each patch has norm 1.  Means that each
    filter will have equal power */
    whitenedX(::, *) :/= norm(whitenedX(*, ::), 2)

    val kMeansModel = KMeansPlusPlusEstimator(numTemplateFeatures, maxIterations).fit(whitenedX)
    val zca = zcaWhitener.whitener
    val W = zca * kMeansModel.means.t // numZCADimensions by numTemplateFeatures
    val b = zcaWhitener.means.t * W

    new MatrixLinearMapperThenMax(W, b.t)
  }
}
