package nodes.misc

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class SparseFeatureVectorizerSuite extends FunSuite with LocalSparkContext with Logging {
  test("sparse feature vectorization") {
    sc = new SparkContext("local", "test")

    val featureVectorizer = new SparseFeatureVectorizer(Map("First" -> 0, "Second" -> 1, "Third" -> 2))
    val test = Seq(("Third", 4.0), ("Fourth", 6.0), ("First", 1.0))
    val vector = featureVectorizer.apply(sc.parallelize(Seq(test))).first()

    assert(vector.size == 3)
    assert(vector(0) == 1)
    assert(vector(1) == 0)
    assert(vector(2) == 4)
  }

  test("all sparse feature selection") {
    sc = new SparkContext("local", "test")
    val train = sc.parallelize(List(Seq(("First", 0.0), ("Second", 6.0)), Seq(("Third", 3.0), ("Second", 4.0))))

    val featureVectorizer = AllSparseFeatures.fit(train.map(x => x))
    // The selected features should now be "First", "Second", and "Third"

    val test = Seq(("Third", 4.0), ("Fourth", 6.0), ("First", 1.0))
    val out = featureVectorizer.apply(sc.parallelize(Seq(test))).first().toArray

    // It is unpredictable what mapping of feature to index will be used by the vectorizer
    // just make sure expected values are found somewhere, and unexpected values not seen
    assert(out.length == 3)
    assert(out.contains(4))
    assert(out.contains(1))
    assert(!out.contains(6))
  }

  test("common sparse feature selection") {
    sc = new SparkContext("local", "test")
    val train = sc.parallelize(List(
      Seq(("First", 0.0), ("Second", 6.0)),
      Seq(("Third", 3.0), ("Second", 4.8)),
      Seq(("Third", 7.0), ("Fourth", 5.0)),
      Seq(("Fifth", 5.0), ("Second", 7.3))
    ))

    val featureVectorizer = CommonSparseFeatures(2).fit(train.map(x => x))
    // The selected features should now be "Second", and "Third"

    val test = Seq(("Third", 4.0), ("Seventh", 8.0), ("Second", 1.3), ("Fourth", 6.0), ("First", 1.0))
    val out = featureVectorizer.apply(sc.parallelize(Seq(test))).first().toArray

    // It is unpredictable what mapping of feature to index will be used by the vectorizer
    // just make sure expected values are found somewhere, and unexpected values not seen
    assert(out.length == 2)
    assert(out.contains(1.3))
    assert(out.contains(4.0))
  }
}
