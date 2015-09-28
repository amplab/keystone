package loaders

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object AmazonReviewsDataLoader {
  /**
   * Loads the Amazon Product Reviews dataset for binary classification.
   * Each review is a JSON string with (at least) two fields: "reviewText" and "overAll".
   *
   * This data loader produces an RDD of labeled reviews.
   *
   * @param sc  SparkContext to use
   * @param dataDir  Directory of the training data
   * @param threshold  Lowest value at which to consider a review positive.
   * @return  A Labeled Dataset that contains the data strings and labels.
   */
  def apply(sc: SparkContext, dataDir: String, threshold: Double): LabeledData[Int, String] = {
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.jsonFile(dataDir)
    val data = df.select(df("overall"), df("reviewText"))
        .map(r => (if(r.getAs[Double](0) >= threshold) 1 else 0, r.getAs[String](1)))

    LabeledData(data)
  }
}
