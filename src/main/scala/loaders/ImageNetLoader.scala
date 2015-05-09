package loaders

import java.io.FileInputStream
import java.io.ByteArrayInputStream
import java.net.URI

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import utils.{ImageMetadata, LabeledImage, ImageUtils}

/**
 * Helper object to loads images from ImageNet Datasets.
 */

object ImageNetLoader {
  
  /**
   * Loads images from @dataPath and associates images with the labels provided in @labelPath
   *
   * @param sc SparkContext to use
   * @param dataPath Directory containing tar files (can be a HDFS path). This classes assumes
   *                 that each tar file contains images within a directory. The name of the
   *                 directory is treated as the className.
   * @param labelsPath Local file that maps classNames to a numeric value
   */
  def apply(sc: SparkContext, dataPath: String, labelsPath: String): RDD[LabeledImage] = {
    val filePaths = FileSystem.get(new URI(dataPath), new Configuration(true))
      .listStatus(new Path(dataPath))
      .filter(x => !x.isDir())
      .map(x => x.getPath().toUri())
    val numParts = filePaths.length
    val filePathsRDD = sc.parallelize(filePaths, numParts)

    val labelsMapFile = scala.io.Source.fromFile(labelsPath)
    val labelsMap = labelsMapFile.getLines().map(x => x.toString).toArray.map { line =>
      val parts = line.split(" ")
      (parts(0), parts(1).toInt)
    }.toMap
    ImageNetLoader.loadFiles(filePathsRDD, labelsMap)
  }

  /**
   * Load all files whose paths are contained in @filePathsRDD.
   * @param filePathsRDD a list of tarfile paths containing images
   * @param labelsMap a mapping from image directory name to image label
   */
  def loadFiles(
      filePathsRDD: RDD[URI],
      labelsMap: Map[String, Int]): RDD[LabeledImage] = {
    filePathsRDD.flatMap(fileUri => loadFile(fileUri, labelsMap))
  }

  private def loadFile(
      fileUri: URI,
      labelsMap: Map[String, Int]): Iterator[LabeledImage] = {
    val filePath = new Path(fileUri)
    val conf = new Configuration(true)
    val fs = FileSystem.get(filePath.toUri(), conf)
    val fStream = fs.open(filePath)

    val tarStream = new ArchiveStreamFactory().createArchiveInputStream(
      "tar", fStream).asInstanceOf[TarArchiveInputStream]

    var entry = tarStream.getNextTarEntry()
    val imgs = new ArrayBuffer[LabeledImage]
    while (entry != null) {
      if (!entry.isDirectory) {
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
        val fileParts = entry.getName.split('/')
        val label = fileParts(0)
        val fileName = fileParts.last

        val image = ImageUtils.loadImage(bais).map { img =>
          LabeledImage(img, labelsMap(label), Some(fileName))
        }

        imgs ++= image
      }
      entry = tarStream.getNextTarEntry()
    }

    imgs.iterator
  }
}
