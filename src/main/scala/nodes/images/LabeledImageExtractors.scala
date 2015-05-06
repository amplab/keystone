package nodes.images

import org.apache.spark.rdd.RDD
import pipelines._
import utils.{Image, LabeledImage}

/**
 * Extracts a label from a labeled image.
 */
case object LabelExtractor extends Transformer[LabeledImage, Int] {
  def apply(in: LabeledImage): Int = in.label
}

/**
 * Extracts an image from a labeled image.
 */
case object ImageExtractor extends Transformer[LabeledImage, Image] {
  def apply(in: LabeledImage): Image = in.image
}