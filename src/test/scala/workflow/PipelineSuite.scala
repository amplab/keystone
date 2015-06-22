package workflow

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class PipelineSuite extends FunSuite with LocalSparkContext with Logging {
  test("pipeline chaining") {
    sc = new SparkContext("local", "test")

    val first = Transformer[Int, Int](_ * 2)
    val second = Transformer[Int, Int](_ - 3)

    val data = sc.parallelize(Seq(32, 94, 12))
    val pipeline = first andThen second

    val pipelineOut = pipeline(data).collect().toSeq

    assert(pipeline(7) === (7 * 2) - 3)
    assert(pipelineOut === Seq((32*2) - 3, (94*2) - 3, (12*2) - 3))
  }

  test("estimator chaining") {
    sc = new SparkContext("local", "test")

    val doubleTransformer = Transformer[Int, Int](_ * 2)

    val intEstimator = new Estimator[Int, Int] {
      protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }


    val data = sc.parallelize(Seq(32, 94, 12))
    val pipeline = doubleTransformer andThen (intEstimator, data)

    val pipelineOut = pipeline(data).collect().toSeq
    val pipelineLastTransformerOut = pipeline.fittedTransformer(data).collect().toSeq

    assert(pipelineOut === Seq(32*2 + 32*2, 94*2 + 32*2, 12*2 + 32*2))
    assert(pipelineLastTransformerOut === Seq(32 + 32*2, 94 + 32*2, 12 + 32*2))
  }

  test("label estimator chaining") {
    sc = new SparkContext("local", "test")

    val doubleTransformer = Transformer[Int, Int](_ * 2)

    val intEstimator = new LabelEstimator[Int, Int, String] {
      protected def fit(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
        val first = data.first() + labels.first().toInt
        Transformer(x => x + first)
      }
    }


    val data = sc.parallelize(Seq(32, 94, 12))
    val labels = sc.parallelize(Seq("10", "7", "14"))
    val pipeline = doubleTransformer andThen (intEstimator, data, labels)

    val pipelineOut = pipeline(data).collect().toSeq
    val pipelineLastTransformerOut = pipeline.fittedTransformer(data).collect().toSeq

    assert(pipelineOut === Seq(32*2 + 32*2 + 10, 94*2 + 32*2 + 10, 12*2 + 32*2 + 10))
    assert(pipelineLastTransformerOut === Seq(32 + 32*2 + 10, 94 + 32*2 + 10, 12 + 32*2 + 10))
  }

  test("Pipeline gather") {
    sc = new SparkContext("local", "test")

    val firstPipeline = Transformer[Int, Int](_ * 2) andThen Transformer[Int, Int](_ - 3)

    val secondPipeline = Transformer[Int, Int](_ * 2) andThen (new Estimator[Int, Int] {
      protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)))

    val thirdPipeline = Transformer[Int, Int](_ * 4) andThen (new LabelEstimator[Int, Int, String] {
      protected def fit(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
        val first = data.first() + labels.first().toInt
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)), sc.parallelize(Seq("10", "7", "14")))

    val pipeline = Pipeline.gather {
      firstPipeline :: secondPipeline :: thirdPipeline :: Nil
    }

    val single = 7
    assert(pipeline(single) === Seq(firstPipeline.apply(single), secondPipeline.apply(single), thirdPipeline.apply(single)))

    val data = Seq(13, 2, 83)
    val correctOut = data.map(x => Seq(firstPipeline.apply(x), secondPipeline.apply(x), thirdPipeline.apply(x)))
    assert(pipeline(sc.parallelize(data)).collect().toSeq === correctOut)
  }
}
