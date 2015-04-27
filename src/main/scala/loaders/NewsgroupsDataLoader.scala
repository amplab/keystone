package loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.reflect.ClassTag

/**
 * A case class containing an RDD of labeled data
 * @tparam Label  The type of the labels
 * @tparam Datum  The type of the data
 */
case class LabeledData[Label : ClassTag, Datum : ClassTag](labeledData: RDD[(Label, Datum)]) {
  val data: RDD[Datum] = labeledData.map(_._2)
  val labels: RDD[Label] = labeledData.map(_._1)
}

/** A case class containing loaded 20 Newsgroups train & test data */
case class NewsgroupsData(classes: Array[String], train: LabeledData[Int, String], test: LabeledData[Int, String])

/**
 * FIXME: ADD DESCRIPTION AHHH!!! AND LINK TO WHERE THE DATA CAN BE DOWNLOADED
 */
object NewsgroupsDataLoader {
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

  def apply(sc: SparkContext, trainDir: String, testDir: String): NewsgroupsData = {
    val trainData: RDD[(Int, String)] = new UnionRDD(sc, classes.zipWithIndex.map{ case (className, index) => {
      sc.wholeTextFiles(s"$trainDir/$className").map(index -> _._2)
    }})

    val testData: RDD[(Int, String)] = new UnionRDD(sc, classes.zipWithIndex.map{ case (className, index) => {
      sc.wholeTextFiles(s"$testDir/$className").map(index -> _._2)
    }})

    NewsgroupsData(classes, LabeledData(trainData), LabeledData(testData))
  }
}
