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
    val filePaths = FileSystem.get(new URI(dataPath.imagesDirName), new Configuration(true))
      .listStatus(new Path(dataPath.imagesDirName))
      .filter(x => !x.isDirectory())
      .map(x => x.getPath().toUri())

    val numParts = dataPath.numParts.getOrElse(filePaths.length)
    val filePathsRDD = sc.parallelize(filePaths, numParts)


    val labelsMapFile = scala.io.Source.fromFile(labelsPath.labelsFileName)

    val labelsMap: Map[String, Array[Int]] = labelsMapFile.getLines().drop(1).map(x => x.toString).map { line =>
      val parts = line.split(",")
      (parts(4).replace("\"", ""), parts(1).toInt - 1)
    }.toArray.groupBy(_._1).mapValues(_.map(_._2)).map(identity)
    labelsMapFile.close()

    loadFiles(filePathsRDD, dataPath.namePrefix, labelsMap)
  }

  /** Load all files whose paths are contained in @filePathsRDD.
    *
    * @param filePathsRDD a list of tarfiles containing images
    * @param labels a mapping from image directory to image label
    */
  def loadFiles(
      filePathsRDD: RDD[URI],
      namePrefix: String,
      labels: Map[String, Array[Int]]): RDD[MultiLabeledImage] = {

    filePathsRDD.flatMap(fileUri => loadFile(fileUri, namePrefix, labels))
  }

  private def loadFile(
      fileUri: URI, namePrefix: String,
      labelsMap: Map[String, Array[Int]]): Iterator[MultiLabeledImage] = {

    val filePath = new Path(fileUri)
    val conf = new Configuration(true)

    val fs = FileSystem.get(filePath.toUri(), conf)
    val fStream = fs.open(filePath)

    val tarStream = new ArchiveStreamFactory().createArchiveInputStream(
      "tar", new GZIPInputStream(fStream)).asInstanceOf[TarArchiveInputStream]

    var entry = tarStream.getNextTarEntry()
    val imgs = new ArrayBuffer[MultiLabeledImage]
    while (entry != null) {
      if (!entry.isDirectory && entry.getName.startsWith(namePrefix)) {
        var offset = 0
        var ret = 0
        val content = new Array[Byte](entry.getSize().toInt)
        while (ret >= 0 && offset != entry.getSize()) {
          ret = tarStream.read(content, offset, content.length - offset)
          if (ret >= 0) {
            offset += ret
          }
        }
        val bais = new ByteArrayInputStream(content)
        val image = loadImage(bais, entry.getName(), labelsMap)
        imgs ++= image
      }
      entry = tarStream.getNextTarEntry()
    }

    imgs.iterator
  }

  def loadImage(
      fileBytes: InputStream,
      fileName: String,
      labelsMap: Map[String, Array[Int]]): Option[MultiLabeledImage] = {

    classOf[ImageIO].synchronized {
      try {
        val label = fileName
        val img = ImageIO.read(fileBytes)
        if (img != null) {
          if (img.getType() == BufferedImage.TYPE_3BYTE_BGR) {
            Some(MultiLabeledImage(ImageConversions.bufferedImageToWrapper(img), labelsMap(label), Some(label) ))
          } else if (img.getType() == BufferedImage.TYPE_BYTE_GRAY) {
            Some(MultiLabeledImage(ImageConversions.grayScaleImageToWrapper(img), labelsMap(label), Some(label) ))
          } else {
            logWarning("Ignoring image: %s, not RGB or Grayscale of type %d".format(
              fileName, img.getType()))
            None
          }
        } else {
          logWarning("Failed to parse image: %s (result was null)".format(fileName))
          None
        }
      } catch {
        case e: Exception =>
          logWarning("Failed to parse image: %s (due to %s)".format(fileName, e.getMessage()))
          None
      }
    }
  }
}


