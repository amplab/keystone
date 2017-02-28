package keystoneml.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.reflect.ClassTag


object NewsgroupsDataLoader {
  /** The 20 Newsgroups class labels (and directory names) **/
  val classes = Array(
    "comp.graphics",
    "comp.os.ms-windows.misc",
    "comp.sys.ibm.pc.hardware",
    "comp.sys.mac.hardware",
    "comp.windows.x",
    "rec.autos",
    "rec.motorcycles",
    "rec.sport.baseball",
    "rec.sport.hockey",
    "sci.crypt",
    "sci.electronics",
    "sci.med",
    "sci.space",
    "misc.forsale",
    "talk.politics.misc",
    "talk.politics.guns",
    "talk.politics.mideast",
    "talk.religion.misc",
    "alt.atheism",
    "soc.religion.christian"
  )

  /**
   * Loads the 20 newsgroups dataset.
   * Designed to load data from 20news-bydate.tar.gz from http://qwone.com/~jason/20Newsgroups/
   *
   * The expected directory structure for the train and test dirs is:
   * train_or_test_dir/class_label/docs_as_separate_plaintext_files
   *
   * @param sc  SparkContext to use
   * @param dataDir  Directory of the training data
   * @return  A NewsgroupsData object containing the loaded train & test data as RDDs
   */
  def apply(sc: SparkContext, dataDir: String): LabeledData[Int, String] = {
    val data: RDD[(Int, String)] = new UnionRDD(sc, classes.zipWithIndex.map{ case (className, index) => {
      sc.wholeTextFiles(s"$dataDir/$className").map(index -> _._2)
    }})

    LabeledData(data)
  }
}
