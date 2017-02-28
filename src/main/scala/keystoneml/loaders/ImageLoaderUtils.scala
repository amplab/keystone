package keystoneml.loaders

import java.awt.image.BufferedImage
import java.io.{InputStream, ByteArrayInputStream}
import java.net.URI
import java.util.zip.GZIPInputStream
import javax.imageio.ImageIO

import keystoneml.loaders.VOCLoader._
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import keystoneml.pipelines.Logging
import keystoneml.utils._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object ImageLoaderUtils extends Logging {
  /**
   * Given a spark context, a path to a directory containing a number of files, and an optional number of parts,
   * return an RDD with one URI per file on the data path.
   *
   * @param sc Spark context.
   * @param dataPath Data path.
   * @param numParts Number of desired partitions of the output (if unset, one partition per file will be created.)
   * @return
   */
  def getFilePathsRDD(sc: SparkContext, dataPath: String, numParts: Option[Int] = None): RDD[URI] = {
    val filePaths = FileSystem.get(new URI(dataPath), new Configuration(true))
      .listStatus(new Path(dataPath))
      .filter(x => !x.isDir())
      .map(x => x.getPath().toUri())

    sc.parallelize(filePaths, numParts.getOrElse(filePaths.length))
  }


  /**
   * Load all files whose paths are contained in @filePathsRDD.
   * @param filePathsRDD a list of tarfile paths containing images
   * @param labelsMap a mapping from image directory name to image label
   */
  def loadFiles[L, I <: AbstractLabeledImage[L] : ClassTag](
      filePathsRDD: RDD[URI],
      labelsMap: String => L,
      imageBuilder: (Image, L, Option[String]) => I, // TODO(etrain): We can probably do this with implicits.
      namePrefix: Option[String] = None): RDD[I] = {
    filePathsRDD.flatMap(fileUri => loadFile(fileUri, labelsMap, imageBuilder, namePrefix))
  }

  private def loadFile[L, I <: AbstractLabeledImage[L]](
      fileUri: URI,
      labelsMap: String => L,
      imageBuilder: (Image, L, Option[String]) => I,
      namePrefix: Option[String]): Iterator[I] = {
    val filePath = new Path(fileUri)
    val conf = new Configuration(true)
    val fs = FileSystem.get(filePath.toUri(), conf)
    val fStream = fs.open(filePath)

    val tarStream = new ArchiveStreamFactory().createArchiveInputStream(
      "tar", fStream).asInstanceOf[TarArchiveInputStream]

    var entry = tarStream.getNextTarEntry()
    val imgs = new ArrayBuffer[I]
    while (entry != null) {
      if (!entry.isDirectory && (namePrefix.isEmpty || entry.getName.startsWith(namePrefix.get))) {
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

        val image = ImageUtils.loadImage(bais).map { img =>
          imageBuilder(img, labelsMap(entry.getName), Some(entry.getName))
        }

        imgs ++= image
      }
      entry = tarStream.getNextTarEntry()
    }

    imgs.iterator
  }
}