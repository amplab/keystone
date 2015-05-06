package nodes.images

import org.apache.spark.rdd.RDD
import pipelines._
import utils.{Image, LabeledImage}

/**
 * Extracts a label from a labeled image.
 */
case object LabelExtractor extends Transformer[LabeledImage, Int] {
  override def apply(in: RDD[LabeledImage]): RDD[Int] = {
    in.map(_.label)
  }
}

/**
 * Extracts an image from a labeled image.
 */
case object ImageExtractor extends Transformer[LabeledImage, Image] {
  override def apply(in: RDD[LabeledImage]): RDD[Image] = {
    in.map(_.image)
  }
}