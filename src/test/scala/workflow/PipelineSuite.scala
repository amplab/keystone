package workflow

import nodes.util.Identity
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

  test("Do not fit estimators multiple times") {
    sc = new SparkContext("local", "test")

    var numFits = 0

    val intTransformer = Transformer[Int, Int](x => x)
    val intEstimator = new Estimator[Int, Int] {
      protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
        numFits = numFits + 1
        Transformer(x => x)
      }
    }


    val data = sc.parallelize(Seq(32, 94, 12))
    val pipeline = intTransformer andThen (intEstimator, data)

    val pipelineOut = pipeline(data).collect().toSeq
    val pipelineOut2 = pipeline(data).collect().toSeq

    assert(numFits === 1, "Estimator should have been fit exactly once")
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
    assert {
      pipeline(single) === Seq(
        firstPipeline.apply(single),
        secondPipeline.apply(single),
        thirdPipeline.apply(single))
    }

    val data = Seq(13, 2, 83)
    val correctOut = data.map { x => Seq(
      firstPipeline.apply(x),
      secondPipeline.apply(x),
      thirdPipeline.apply(x))
    }
    assert(pipeline(sc.parallelize(data)).collect().toSeq === correctOut)
  }

  test("Lazy execution") {
    sc = new SparkContext("local", "test")

    def gatherToTransformer[A, B](branches: Seq[Pipeline[A, _]], transformerNode: TransformerNode): Pipeline[A, B] = {
      // attach a value per branch to offset all existing node ids by.
      val branchesWithNodeOffsets = branches.scanLeft(0)(_ + _.nodes.size).zip(branches)

      val newNodes = branches.map(_.nodes).reduceLeft(_ ++ _) :+ transformerNode

      val newDataDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
        val dataDeps = branch.dataDeps
        dataDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
      }.reduceLeft(_ ++ _) :+  branchesWithNodeOffsets.map { case (offset, branch) =>
        val sink = branch.sink
        if (sink == Pipeline.SOURCE) Pipeline.SOURCE else sink + offset
      }

      val newFitDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
        val fitDeps = branch.fitDeps
        fitDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
      }.reduceLeft(_ ++ _) :+  None

      val newSink = newNodes.size - 1
      Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
    }

    val accessNoneNode = new TransformerNode {
      override private[workflow] def transform(dataDependencies: Iterator[_]): Any = 0
      override private[workflow] def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[_] = sc.parallelize(Seq(0))
    }

    val accessOneNode = new TransformerNode {
      override private[workflow] def transform(dataDependencies: Iterator[_]): Any = {
        dataDependencies.next()
        0
      }
      override private[workflow] def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[_] = {
        dataDependencies.next()
        sc.parallelize(Seq(0))
      }
    }

    val accessTwoNode = new TransformerNode {
      override private[workflow] def transform(dataDependencies: Iterator[_]): Any = {
        dataDependencies.next()
        dataDependencies.next()
        0
      }
      override private[workflow] def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[_] = {
        dataDependencies.next()
        dataDependencies.next()
        sc.parallelize(Seq(0))
      }
    }

    val mutableState = Array(0)
    val data = sc.parallelize(Seq(0))

    val branchOne = Estimator[Int, Int] { _ =>
      mutableState(0) = mutableState(0) + 1
      new Identity[Int]
    }.withData(data)

    val branchTwo = Estimator[Int, Int] { _ =>
      mutableState(0) = mutableState(0) - 2
      Transformer(_ + 1)
    }.withData(data)

    val pipeNone = gatherToTransformer[Int, Int](Seq(branchOne, branchTwo), accessNoneNode)
    val pipeOne = gatherToTransformer[Int, Int](Seq(branchOne, branchTwo), accessOneNode)
    val pipeTwo = gatherToTransformer[Int, Int](Seq(branchOne, branchTwo), accessTwoNode)

    assert(mutableState(0) === 0)

    pipeNone(data)
    assert(mutableState(0) === 0)
    mutableState(0) = 0

    pipeOne(data)
    assert(mutableState(0) === 1)
    mutableState(0) = 0

    pipeTwo(data)
    assert(mutableState(0) === -1)
    mutableState(0) = 0
  }
}
