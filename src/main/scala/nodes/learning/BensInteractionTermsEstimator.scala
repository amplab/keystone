package nodes.learning

import breeze.linalg._
import breeze.numerics.{log => bLog}
import nodes.stats.{ColumnSampler, GaussianRandomFeatures, MatrixLinearMapperThenMax}
import org.apache.spark.rdd.RDD
import pipelines.{Transformer, Estimator, Logging}
import utils.MatrixUtils

class BensInteractionTerms(
    templateFilters: MatrixLinearMapperThenMax,
    gaussianFilters: MatrixLinearMapperThenMax)
  extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {

  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    val template = templateFilters.apply(in)
    val gaussian = gaussianFilters.apply(in)
    (template * gaussian.t) :/= in.cols.toDouble
  }
}

case class BensInteractionTermsEstimator(numTemplateFilters: Int, numGaussianFilters: Int, numSamples: Int, maxIterations: Int = 100) extends Estimator[DenseMatrix[Double], DenseMatrix[Double]] with Logging {
  require(maxIterations > 0, "maxIterations must be positive")

  def fit(samples: RDD[DenseMatrix[Double]]): BensInteractionTerms = {
    val x: Array[DenseVector[Double]] = new ColumnSampler(numSamples).apply(samples.map(x => convert(x, Float))).map(x => convert(x, Double)).collect()
    fit(MatrixUtils.rowsToMatrix(x))
  }

  def fit(X: DenseMatrix[Double]): BensInteractionTerms = {
    val zcaWhitener = new ZCAWhitenerEstimator().fitSingle(X)

    val templateFilters = BensTemplateFiltersEstimator(numTemplateFilters, zcaWhitener, maxIterations).fit(X)
    val gaussianFilters = GaussianRandomFeatures(numGaussianFilters, zcaWhitener)

    new BensInteractionTerms(templateFilters, gaussianFilters)
  }
}
