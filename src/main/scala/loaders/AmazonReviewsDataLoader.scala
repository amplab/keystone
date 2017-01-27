package loaders

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


object AmazonReviewsDataLoader {
  /**
   * Loads the Amazon Product Reviews dataset for binary classification.
   * Each review is a JSON string with (at least) two fields: "reviewText" and "overAll".
   *
   * This data loader produces an RDD of labeled reviews.
   *
   * @param spark  SparkSession to use (needed for SQL)
   * @param dataDir  Directory of the training data
   * @param threshold  Lowest value at which to consider a review positive.
   * @return  A Labeled Dataset that contains the data strings and labels.
   */
  def apply(spark: SparkSession, dataDir: String, threshold: Double): LabeledData[Int, String] = {
    import spark.implicits._

    val df = spark.read.json(dataDir)
    val data = df.select(df("overall"), df("reviewText"))
        .map(r => (if(r.getAs[Double](0) >= threshold) 1 else 0, r.getAs[String](1))).rdd

    LabeledData(data)
  }
}
