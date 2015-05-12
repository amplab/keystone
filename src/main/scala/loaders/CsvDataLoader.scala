package loaders

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Data Loader that loads csv files of comma separated numbers into an RDD of DenseVectors
 */
object CsvDataLoader {
  /**
   * Load CSV files from the given path into an RDD of DenseVectors
   * @param sc The spark context to use
   * @param path The path to the CSV files
   * @return RDD of DenseVectors, one per CSV row
   */
  def apply(sc: SparkContext, path: String): RDD[DenseVector[Double]] = {
    sc.textFile(path).map(row => DenseVector(row.split(",").map(_.toDouble)))
  }

  /**
   * Load CSV files from the given path into an RDD of DenseVectors
   * @param sc The spark context to use
   * @param path The path to the CSV files
   * @param minPartitions The minimum # of partitions to use
   * @return RDD of DenseVectors, one per CSV row
   */
  def apply(sc: SparkContext, path: String, minPartitions: Int): RDD[DenseVector[Double]] = {
    sc.textFile(path, minPartitions).map(row => DenseVector(row.split(",").map(_.toDouble)))
  }
}
