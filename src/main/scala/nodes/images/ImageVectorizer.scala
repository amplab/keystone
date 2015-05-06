package nodes.images

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines._
import utils.Image

/**
 * Takes an image and converts it to a dense vector.
 */
case object ImageVectorizer extends Transformer[Image, DenseVector[Double]] {
  override def apply(in: RDD[Image]): RDD[DenseVector[Double]] = {
    in.map(im => DenseVector(im.toArray))
  }
}