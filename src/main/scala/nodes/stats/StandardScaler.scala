package nodes.stats

import breeze.linalg.DenseVector
import breeze.numerics.sqrt
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Transformer}
import utils.MLlibUtils

/**
 * Represents a StandardScaler model that can transform dense vectors.
 *
 * @param mean column mean values
 * @param std column standard deviation values
 */
class StandardScalerModel(val mean: DenseVector[Double], val std: Option[DenseVector[Double]] = None)
    extends Transformer[DenseVector[Double], DenseVector[Double]] {
  /**
   * Applies standardization transformation on a vector.
   *
   * @param in Vector to be standardized.
   * @return Standardized vector. If the std of a column is zero, it will return default `0.0`
   *         for the column with zero std.
   */
  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val out = in - mean
    std.foreach(x => {
      out :/= x
    })
    out
  }
}

/**
 * Standardizes features by removing the mean and scaling to unit std using column summary
 * statistics on the samples in the training set.
 */
class StandardScaler(normalizeStdDev: Boolean = true, eps: Double = 1E-12) extends Estimator[DenseVector[Double], DenseVector[Double]]{
  /**
   * Computes the mean and variance and stores as a model to be used for later scaling.
   *
   * @param data The data used to compute the mean and variance to build the transformation model.
   * @return a StandardScalarModel
   */
  override def fit(data: RDD[DenseVector[Double]]): StandardScalerModel = {
    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(MLlibUtils.breezeVectorToMLlib(data)),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    if (normalizeStdDev) {
      new StandardScalerModel(
        MLlibUtils.mllibVectorToDenseBreeze(summary.mean),
        Some(sqrt(MLlibUtils.mllibVectorToDenseBreeze(summary.variance))
            .map(r => if (r.isNaN | r.isInfinite | math.abs(r) < eps) 1.0 else r)))
    } else {
      new StandardScalerModel(
        MLlibUtils.mllibVectorToDenseBreeze(summary.mean),
        None)
    }
  }
}
