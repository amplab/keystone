package loaders

import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.util.zip.GZIPInputStream
import javax.imageio.ImageIO

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines.{Logging, Transformer}
import utils.{MultiLabeledImage, Image, ImageConversions}

import scala.collection.mutable.ArrayBuffer


case class VOCDataPath(imagesDirName: String, namePrefix: String, numParts: Option[Int])
case class VOCLabelPath(labelsFileName: String)

/**
 * A data loader for the VOC 2007 Dataset. Expects input in a tar file.
 */
object VOCLoader extends Logging with Serializable {
  val NUM_CLASSES = 20 // This is a constant defined by the VOC 2007 dataset.

  /**
   * Loads a data path given a spark context and labels and returns an RDD[MultiLabeledImage].
   *
   * A property of the VOC dataset is that images can have multiple labels which we
   * have to deal with later in the pipeline.
   *
   * @param sc A Spark Context
   * @param dataPath Path to image tar.
   * @param labelsPath Path to label csv.
   * @return
   */
  def apply(sc: SparkContext, dataPath: VOCDataPath, labelsPath: VOCLabelPath): RDD[MultiLabeledImage] = {
    val filePathsRDD = ImageLoaderUtils.getFilePathsRDD(sc, dataPath.imagesDirName, dataPath.numParts)

    val labelsMapFile = scala.io.Source.fromFile(labelsPath.labelsFileName)

    val labelsMap: Map[String, Array[Int]] = labelsMapFile
      .getLines()
      .drop(1)
      .map(x => x.toString)
      .map { line =>
        val parts = line.split(",")
        (parts(4).replace("\"", ""), parts(1).toInt - 1)
      }
      .toArray
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(identity)

    labelsMapFile.close()

    ImageLoaderUtils.loadFiles(filePathsRDD, labelsMap, MultiLabeledImage.apply, Some(dataPath.namePrefix))
  }
}


