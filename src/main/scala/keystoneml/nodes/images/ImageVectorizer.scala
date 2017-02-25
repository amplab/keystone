package keystoneml.nodes.images

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import keystoneml.pipelines._
import keystoneml.utils.Image
import keystoneml.workflow.Transformer

/**
 * Takes an image and converts it to a dense vector.
 */
object ImageVectorizer extends Transformer[Image, DenseVector[Double]] {
  def apply(in: Image): DenseVector[Double] = {
    DenseVector(in.toArray)
  }
}