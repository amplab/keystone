package nodes

import pipelines._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.util.zip.GZIPInputStream
import java.io.{BufferedInputStream, FileInputStream}

/**
 * Loads images from the CIFAR-10 dataset.
 */

class CifarLoader
    extends PipelineNode[(SparkContext, String), RDD[LabeledImage]]
    with Serializable {

  def apply(in: (SparkContext, String)): RDD[LabeledImage] = {
    val sc = in._1
    val path = in._2

    val images = CifarLoader.loadLabeledImages(path)

    sc.parallelize(images)
  }

}

object CifarLoader {
  def loadLabeledImages(path: String): Seq[LabeledImage] = {
    //We hardcode this because these are properties of the CIFAR-10 dataset.
    val nrow = 32
    val ncol = 32
    val nchan = 3
    val labelSize = 1

    val imgCount = labelSize + nrow*ncol*nchan

    val imageBytes = Array.fill[Byte](imgCount)(0x00)
    var out = Array[LabeledImage]()

    val inFile = new FileInputStream(path)

    println("About to read image")
    while(inFile.read(imageBytes, 0, imgCount) > 0) {
      val img = cifar10ToBufferedImage(imageBytes.tail)
      val label = imageBytes.head.toShort
      val li = LabeledImage(img, label)
      out = out :+ li
    }
    out
  }
}
