package keystoneml.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import keystoneml.utils.LabeledImage

/**
 * Helper object to loads images from ImageNet Datasets.
 */

object ImageNetLoader {

  val NUM_CLASSES = 1000
  
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
    val filePathsRDD = ImageLoaderUtils.getFilePathsRDD(sc, dataPath)

    val labelsMapFile = scala.io.Source.fromFile(labelsPath)
    val labelsMap = labelsMapFile.getLines().map(x => x.toString).toArray.map { line =>
      val parts = line.split(" ")
      (parts(0), parts(1).toInt)
    }.toMap

    def labelsMapF(fname: String): Int = {
      labelsMap(fname.split('/')(0))
    }

    ImageLoaderUtils.loadFiles(filePathsRDD, labelsMapF, LabeledImage.apply)
  }
}
