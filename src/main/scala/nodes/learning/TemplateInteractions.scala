package nodes.learning

import breeze.linalg._
import breeze.numerics.{log => bLog}
import nodes.stats.{ColumnSampler, GaussianRandomFeatures, MatrixLinearMapperThenMax}
import org.apache.spark.rdd.RDD
import pipelines.{Transformer, Estimator, Logging}
import utils.MatrixUtils

// 
class TemplateInteractions(
    kitchenSinkGaussian: (DenseMatrix[Double], DenseVector[Double]),
    kitchenSinkTemplates: (DenseMatrix[Double], DenseVector[Double]),
    gaussianOffset: Double = 0.0,
    templateOffset: Double = 0.25)
  extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {

  def maxInPlace(in: DenseMatrix[Double], eps:Double=0.0): DenseMatrix[Double] = {
    var i = 0
    while (i < in.rows) {
      var j = 0
      while (j < in.cols) {
        in(i, j) = max(in(i, j), eps)
        j = j + 1
      }
      i = i + 1
    }
    in
  }

  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    // in is numSifts (128) * numKeyPoints (10k)
    val gaussianFeatures = in.t * kitchenSinkGaussian._1
    gaussianFeatures(*, ::) -= kitchenSinkGaussian._2
    maxInPlace(gaussianFeatures, gaussianOffset)

    val templateFeatures = in.t * kitchenSinkTemplates._1
    templateFeatures(*, ::) -= kitchenSinkTemplates._2
    maxInPlace(templateFeatures, templateOffset)

    // 256 by 16
    val features = gaussianFeatures.t * templateFeatures
    features :/= gaussianFeatures.rows.toDouble
    features
  }

}
