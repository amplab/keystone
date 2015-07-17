package loaders

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/** A case class containing loaded pre-featurized TIMIT train & test data */
case class TimitFeaturesData(
  train: LabeledData[Int, DenseVector[Double]],
  test: LabeledData[Int, DenseVector[Double]]
)

object TimitFeaturesDataLoader {
  val timitDimension = 440
  val numClasses = 147

  // Assumes lines are formatted as
  // row col value
  private def parseSparseLabels(fileName: String) = {
    // Mapping from row number to label
    val ret = new mutable.HashMap[Long, Int]

    val lines = scala.io.Source.fromFile(fileName).getLines()
    lines.foreach { line =>
      val parts = line.split(" ")
      ret(parts(0).toLong - 1) = parts(1).toInt
    }
    ret
  }

  private def createLabelsRDD(
      labelsMap: mutable.HashMap[Long, Int],
      featuresRDD: RDD[_]) = {
    val labelsMapBC = featuresRDD.context.broadcast(labelsMap)
    val labelsRDD = featuresRDD.zipWithIndex().map { case (item, row) =>
      labelsMapBC.value(row) - 1
    }
    labelsRDD
  }

  /**
   * Loads the pre-featurized Timit data.
   * Expects features data to be stored as a csv of numbers,
   * and labels as "row# label" where row# is the number of the row in the data csv it is
   * referring to (starting at row #1)
   *
   * @param sc  SparkContext to use
   * @param trainDataLocation  CSV of the training data
   * @param trainLabelsLocation  labels of the training data
   * @param testDataLocation  CSV of the test data
   * @param testLabelsLocation  labels of the test data
   * @param numParts  number of partitions per RDD
   * @return  A TimitFeaturesData object containing the loaded train & test data as RDDs
   */
  def apply(sc: SparkContext,
      trainDataLocation: String,
      trainLabelsLocation: String,
      testDataLocation: String,
      testLabelsLocation: String,
      numParts: Int = 512): TimitFeaturesData = {
    val trainData = CsvDataLoader(sc, trainDataLocation, numParts)
    val trainLabels = createLabelsRDD(parseSparseLabels(trainLabelsLocation), trainData)

    val testData = CsvDataLoader(sc, testDataLocation, numParts)
    val testLabels = createLabelsRDD(parseSparseLabels(testLabelsLocation), testData)
    TimitFeaturesData(LabeledData(trainLabels.zip(trainData)), LabeledData(testLabels.zip(testData)))
  }
}
