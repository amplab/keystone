package nodes.images

import pipelines._
import utils.{MultiLabeledImage, Image, LabeledImage}

/**
 * Extracts a label from a labeled image.
 */
object LabelExtractor extends Transformer[LabeledImage, Int] {
  def apply(in: LabeledImage): Int = in.label
}

/**
 * Extracts an image from a labeled image.
 */
object ImageExtractor extends Transformer[LabeledImage, Image] {
  def apply(in: LabeledImage): Image = in.image
}

/**
 * Extracts a label from a multi-labeled image.
 */
object MultiLabelExtractor extends Transformer[MultiLabeledImage, Array[Int]] {
  override def apply(in: MultiLabeledImage): Array[Int] = in.label
}

/**
 * Extracts an image from a multi-labeled image.
 */
object MultiLabeledImageExtractor extends Transformer[MultiLabeledImage, Image] {
  def apply(in: MultiLabeledImage): Image = in.image
}
