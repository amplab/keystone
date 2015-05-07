package nodes.images

import breeze.linalg.DenseVector
import breeze.numerics.pow
import pipelines._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import utils.{ArrayVectorizedImage, Image}


class FeatureNormalizer(val mean: DenseVector[Double], val sd: Option[DenseVector[Double]] = None)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {

  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    sd match {
      case Some(s) => (in - mean)/s
      case None => (in - mean)
    }
  }
}

class FeatureNormalize(
    val eps: Double = 1e-12,
    val numFeatures: Option[Int] = None,
    useSd: Boolean = true)
  extends Estimator[DenseVector[Double], DenseVector[Double]] {

  def getMean(in: RDD[DenseVector[Double]]): (DenseVector[Double], Long) = {
    val nFeatures = numFeatures match {
      case Some(x) => x
      case None => in.first.length
    }
    val sumCount = in.map(x => (x, 1L)).aggregate((DenseVector.zeros[Double](nFeatures),0L))(FeatureNormalize.plusMerge,
      FeatureNormalize.plusMerge)

    val count = sumCount._2
    (sumCount._1 / count.toDouble, count)
  }

  def getSd(in: RDD[DenseVector[Double]], mean: DenseVector[Double], count: Long): DenseVector[Double] = {
    val nFeatures = numFeatures match {
      case Some(x) => x
      case None => in.first.length
    }

    val variance = in.map(a => pow(a - mean, 2.0)).aggregate(new DenseVector[Double](nFeatures))(_ + _, _ + _)
    pow(variance / count.toDouble, 0.5).map(r => if(r.isNaN | r.isInfinite | math.abs(r) < eps) 1.0 else r)
  }

  def fit(in: RDD[DenseVector[Double]]): FeatureNormalizer = {
    val (mean, count) = getMean(in)
    val sd = if(useSd) Some(getSd(in, mean, count)) else None

    new FeatureNormalizer(mean, sd)
  }
}

object FeatureNormalize {
  def plusMerge(a: (DenseVector[Double], Long), b: (DenseVector[Double], Long)) = {
    (a._1 + b._1, a._2 + b._2)
  }

}