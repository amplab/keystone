package workflow.graph

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class PipelineSuite extends FunSuite with LocalSparkContext with Logging {
  test("pipeline chaining") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val first = Transformer[Int, Int](_ * 2)
    val second = Transformer[Int, Int](_ - 3)

    val data = sc.parallelize(Seq(32, 94, 12))
    val pipeline = first andThen second

    val pipelineOut = pipeline(data).get().collect().toSeq

    assert(pipeline(7).get() === (7 * 2) - 3)
    assert(pipelineOut === Seq((32*2) - 3, (94*2) - 3, (12*2) - 3))

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Do not fit estimators multiple times") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    var numFits = 0

    val intTransformer = Transformer[Int, Int](x => x)
    val intEstimator = new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        numFits = numFits + 1
        Transformer(x => x)
      }
    }


    val data = sc.parallelize(Seq(32, 94, 12))
    val pipeline = intTransformer andThen (intEstimator, data)

    val pipelineOut = pipeline(data)
    val pipelineOut2 = pipeline(data)

    pipelineOut.get().collect()
    pipelineOut2.get().collect()

    assert(numFits === 1, "Estimator should have been fit exactly once")

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("estimator chaining") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val doubleTransformer = Transformer[Int, Int](_ * 2)

    val intEstimator = new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }


    val data = sc.parallelize(Seq(32, 94, 12))

    val features = doubleTransformer(data)
    val model = intEstimator.withData(features)

    val pipelineChainOne = doubleTransformer andThen (intEstimator, data)
    val pipelineChainTwo = doubleTransformer andThen model

    val pipelineOutOne = pipelineChainOne(data)
    val pipelineOutTwo = pipelineChainTwo(data)
    val modelOut = model(data)

    assert(pipelineOutOne.get().collect().toSeq === Seq(32*2 + 32*2, 94*2 + 32*2, 12*2 + 32*2))
    assert(pipelineOutTwo.get().collect().toSeq === Seq(32*2 + 32*2, 94*2 + 32*2, 12*2 + 32*2))
    assert(modelOut.get().collect().toSeq === Seq(32 + 32*2, 94 + 32*2, 12 + 32*2))

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("label estimator chaining") {
    // Clean global pipeline state
    Pipeline.state.clear()

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

    val features = doubleTransformer(data)
    val model = intEstimator.withData(features, labels)

    val pipelineChainOne = doubleTransformer andThen (intEstimator, data, labels)
    val pipelineChainTwo = doubleTransformer andThen model

    val pipelineOutOne = pipelineChainOne(data)
    val pipelineOutTwo = pipelineChainTwo(data)
    val modelOut = model(data)

    assert(pipelineOutOne.get().collect().toSeq === Seq(32*2 + 32*2 + 10, 94*2 + 32*2 + 10, 12*2 + 32*2 + 10))
    assert(pipelineOutTwo.get().collect().toSeq === Seq(32*2 + 32*2 + 10, 94*2 + 32*2 + 10, 12*2 + 32*2 + 10))
    assert(modelOut.get().collect().toSeq === Seq(32 + 32*2 + 10, 94 + 32*2 + 10, 12 + 32*2 + 10))

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Incrementally update execution state variation 1") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val accum = sc.accumulator(0)
    val intTransformer = Transformer[Int, String](x => {
      accum += 1
      (x * 3).toString
    })
    val intEstimator = new Estimator[String, String] {
      def fit(data: RDD[String]): Transformer[String, String] = {
        Transformer(x => x + "qub")
      }
    }

    val data = sc.parallelize(Seq(32, 94, 12))

    val featurizer = intTransformer andThen Cacher()
    val features = featurizer(data)
    assert(features.get().collect() === Array("96", "282", "36"))
    assert(accum.value === 3, "Incremental code should not have reprocessed cached values")

    val pipe = featurizer andThen intEstimator.withData(features)
    val out = pipe(data)
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(pipe(data).get().collect() === Array("96qub", "282qub", "36qub"))
    assert(accum.value === 3, "Incremental code should not have reprocessed cached values")

    val testData = sc.parallelize(Seq(32, 94))
    val testOut = pipe(testData)
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(accum.value === 5, "Incremental code should not have reprocessed cached values")

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Incrementally update execution state variation 2") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val accum = sc.accumulator(0)
    val intTransformer = Transformer[Int, String](x => {
      accum += 1
      (x * 3).toString
    })
    val intEstimator = new Estimator[String, String] {
      def fit(data: RDD[String]): Transformer[String, String] = {
        Transformer(x => x + "qub")
      }
    }

    val data = sc.parallelize(Seq(32, 94, 12))

    val featurizer = intTransformer andThen Cacher()
    val features = featurizer(data)
    assert(features.get().collect() === Array("96", "282", "36"))
    assert(accum.value === 3, "Incremental code should not have reprocessed cached values")

    val testData = sc.parallelize(Seq(32, 94))
    val testFeatures = featurizer(testData)
    assert(testFeatures.get().collect() === Array("96", "282"))
    assert(accum.value === 5, "Incremental code should not have reprocessed cached values")

    val model = intEstimator.withData(features)

    val out = model(features)
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(accum.value === 5, "Incremental code should not have reprocessed cached values")

    val testOut = model(testFeatures)
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(accum.value === 5, "Incremental code should not have reprocessed cached values")

    val datumOut = model(featurizer(2))
    assert(datumOut.get() === "6qub")
    assert(datumOut.get() === "6qub")
    assert(accum.value === 6, "single uncached value run")

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Incrementally update execution state with LabelEstimator") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val accum = sc.accumulator(0)
    val intTransformer = Transformer[Int, String](x => {
      accum += 1
      (x * 3).toString
    })
    val intEstimator = new LabelEstimator[String, String, String] {
      protected def fit(data: RDD[String], labels: RDD[String]): Transformer[String, String] = {
        Transformer(x => x + "qub")
      }
    }

    val data = sc.parallelize(Seq(32, 94, 12))
    val labels = data.map(_ * 2)

    val featurizer = intTransformer andThen Cacher()
    val features = featurizer(data)
    assert(features.get().collect() === Array("96", "282", "36"))
    assert(accum.value === 3, "Incremental code should not have reprocessed cached values")

    val labelFeatures = featurizer(labels)
    assert(labelFeatures.get().collect() === Array("192", "564", "72"))
    assert(accum.value === 6, "Incremental code should not have reprocessed cached values")

    val pipe = featurizer andThen intEstimator.withData(features, labelFeatures)
    val out = pipe(data)
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(out.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(pipe(data).get().collect() === Array("96qub", "282qub", "36qub"))
    assert(accum.value === 6, "Incremental code should not have reprocessed cached values")

    val labelsOut = pipe(labels)
    assert(labelsOut.get().collect() === Array("192qub", "564qub", "72qub"))
    assert(labelsOut.get().collect() === Array("192qub", "564qub", "72qub"))
    assert(pipe(labels).get().collect() === Array("192qub", "564qub", "72qub"))
    assert(accum.value === 6, "Incremental code should not have reprocessed cached values")

    val testData = sc.parallelize(Seq(32, 94))
    val testOut = pipe(testData)
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(accum.value === 8, "Incremental code should not have reprocessed cached values")

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Incrementally update execution state when andThen is used") {
    // Clean global pipeline state
    Pipeline.state.clear()

    val defaultOptimizer = Pipeline.getOptimizer

    sc = new SparkContext("local", "test")

    // Construct transformers & accumulators to track how much they have processed
    val transformerAccum1 = sc.accumulator(0)
    val transformerAccum2 = sc.accumulator(0)
    val transformer1 = Transformer[String, String](x => {
      transformerAccum1 += 1
      x + "d"
    })
    val transformer2 = Transformer[String, String](x => {
      transformerAccum2 += 1
      x + "e"
    })

    // Construct estimators & accumulators to track how much they have processed
    val estAccum1 = sc.accumulator(0)
    val estAccum2 = sc.accumulator(0)
    val estimator1 = new Estimator[String, String] {
      def fit(data: RDD[String]): Transformer[String, String] = {
        data.foreach { _ =>
          estAccum1 += 1
        }
        Transformer(x => x + "abc")
      }
    }
    val estimator2 = new Estimator[String, String] {
      def fit(data: RDD[String]): Transformer[String, String] = {
        data.foreach { _ =>
          estAccum2 += 1
        }
        Transformer(x => x + "xyz")
      }
    }

    val data1 = sc.parallelize(Seq("h", "i", "j"))
    val data2 = sc.parallelize(Seq("f", "g"))

    // We construct the two halves of the pipeline
    val pipeLeft = transformer1 andThen Cacher() andThen (estimator1, data1)
    val pipeRight = transformer2 andThen Cacher() andThen (estimator2, data2)

    // Nothing should have been executed yet
    assert(transformerAccum1.value === 0)
    assert(transformerAccum2.value === 0)
    assert(estAccum1.value === 0)
    assert(estAccum2.value === 0)

    // Should fit estimator1, then reuse the cached transformer1 result
    assert(pipeLeft(data1).get().collect() === Array("hdabc", "idabc", "jdabc"))
    assert(transformerAccum1.value === 3)
    assert(transformerAccum2.value === 0)
    assert(estAccum1.value === 3)
    assert(estAccum2.value === 0)

    // Should fit estimator2, then reuse the cached transformer2 result
    assert(pipeRight(data2).get().collect() === Array("fexyz", "gexyz"))
    assert(transformerAccum1.value === 3)
    assert(transformerAccum2.value === 2)
    assert(estAccum1.value === 3)
    assert(estAccum2.value === 2)

    // Chain the two pipeline halves
    val pipe = pipeLeft andThen pipeRight

    // Should reuse all fit estimators, and the cached data at transformer1. Must compute at transformer2
    assert(pipe(data1).get().collect() === Array("hdabcexyz", "idabcexyz", "jdabcexyz"))
    assert(transformerAccum1.value === 3)
    assert(transformerAccum2.value === 5)
    assert(estAccum1.value === 3)
    assert(estAccum2.value === 2)

    // Should reuse all fit estimators. Must compute at transformer1 and transformer2. Will not reoptimize
    assert(pipe(data2).get().collect() === Array("fdabcexyz", "gdabcexyz"))
    assert(transformerAccum1.value === 5)
    assert(transformerAccum2.value === 7)
    assert(estAccum1.value === 3)
    assert(estAccum2.value === 2)

    // Now use a datum. Should reuse all fit estimators. Must compute at transformer1 and transformer2.
    // Will not reoptimize
    assert(pipe("l").get() === "ldabcexyz")
    assert(transformerAccum1.value === 6)
    assert(transformerAccum2.value === 8)
    assert(estAccum1.value === 3)
    assert(estAccum2.value === 2)

    // Restore the default optimizer
    Pipeline.setOptimizer(defaultOptimizer)

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("access features and final value") {
    // Clean global pipeline state
    Pipeline.state.clear()

    val defaultOptimizer = Pipeline.getOptimizer

    // Set an optimizer that counts how many times optimization has been executed
    val numOptimizations = new AtomicInteger(0)
    Pipeline.setOptimizer(new Optimizer {
      override protected val batches: Seq[Batch] =
        Batch("Load Saved State", Once, ExtractSaveablePrefixes, SavedStateLoadRule, DanglingNodeRemovalRule) ::
        Batch("Common Sub-expression Elimination", FixedPoint(Int.MaxValue), EquivalentNodeMergeRule) ::
          Batch("Update num-optimizations", Once, new Rule {
            override def apply(plan: Graph, prefixes: Map[NodeId, Prefix]):
            (Graph, Map[NodeId, Prefix]) = {
              numOptimizations.addAndGet(1)
              (plan, prefixes)
            }
          }) ::
          Nil
    })

    sc = new SparkContext("local", "test")

    val accum = sc.accumulator(0)
    val intTransformer = Transformer[Int, String](x => {
      accum += 1
      (x * 3).toString
    })
    val intEstimator = new Estimator[String, String] {
      def fit(data: RDD[String]): Transformer[String, String] = {
        Transformer(x => x + "qub")
      }
    }

    val data = sc.parallelize(Seq(32, 94, 12))
    val testData = sc.parallelize(Seq(32, 94))

    val pipe = intTransformer andThen Cacher() andThen (intEstimator, data)
    val featurizer = intTransformer andThen Cacher()

    val features = featurizer(data)
    val trainOut = pipe(data)
    val testOut = pipe(testData)

    assert(numOptimizations.get() === 0, "Nothing should have been optimized yet")
    assert(accum.value === 0, "Nothing should have been executed yet")


    assert(trainOut.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(trainOut.get().collect() === Array("96qub", "282qub", "36qub"))
    assert(accum.value === 3, "train features should have been processed exactly once")
    assert(numOptimizations.get() === 1, "An optimization should have occurred")

    assert(features.get().collect() === Array("96", "282", "36"))
    assert(accum.value === 3, "train features should have been processed exactly once")
    assert(numOptimizations.get() === 2, "Another optimization should have occurred")

    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(testOut.get().collect() === Array("96qub", "282qub"))
    assert(accum.value === 5, "train and test features should have been processed once each")
    assert(numOptimizations.get() === 3, "Another optimization should have occurred")

    // Restore the default optimizer
    Pipeline.setOptimizer(defaultOptimizer)

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Pipeline gather") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val firstPipeline = Transformer[Int, Int](_ * 2) andThen Transformer[Int, Int](_ - 3)

    val secondPipeline = Transformer[Int, Int](_ * 2) andThen (new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)))

    val thirdPipeline = Transformer[Int, Int](_ * 4) andThen (new LabelEstimator[Int, Int, String] {
      def fit(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
        val first = data.first() + labels.first().toInt
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)), sc.parallelize(Seq("10", "7", "14")))

    val pipeline = Pipeline.gather {
      firstPipeline :: secondPipeline :: thirdPipeline :: Nil
    }

    val single = 7
    assert {
      pipeline(single).get() === Seq(
        firstPipeline.apply(single).get(),
        secondPipeline.apply(single).get(),
        thirdPipeline.apply(single).get())
    }

    val data = Seq(13, 2, 83)
    val correctOut = data.map { x => Seq(
      firstPipeline.apply(x).get(),
      secondPipeline.apply(x).get(),
      thirdPipeline.apply(x).get())
    }
    assert(pipeline(sc.parallelize(data)).get().collect().toSeq === correctOut)

    // Clean global pipeline state
    Pipeline.state.clear()
  }

  test("Pipeline gather incremental construction") {
    // Clean global pipeline state
    Pipeline.state.clear()

    sc = new SparkContext("local", "test")

    val numEstimations = new AtomicInteger(0)

    val firstPipeline = Transformer[Int, Int](_ * 2) andThen Transformer[Int, Int](_ - 3)

    val secondPipeline = Transformer[Int, Int](_ * 2) andThen (new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        numEstimations.addAndGet(1)
        val first = data.first()
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)))

    val thirdPipeline = Transformer[Int, Int](_ * 4) andThen (new LabelEstimator[Int, Int, String] {
      def fit(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
        numEstimations.addAndGet(1)
        val first = data.first() + labels.first().toInt
        Transformer(x => x + first)
      }
    }, sc.parallelize(Seq(32, 94, 12)), sc.parallelize(Seq("10", "7", "14")))

    // Execute pipelines 1 to 3
    assert(numEstimations.get() === 0, "Nothing may have been fit yet")
    firstPipeline(4).get()
    secondPipeline(4).get()
    thirdPipeline(4).get()
    assert(numEstimations.get() === 2, "Estimators must have been fit by now")

    val pipeline = Pipeline.gather {
      firstPipeline :: secondPipeline :: thirdPipeline :: Nil
    }

    val single = 7
    assert {
      pipeline(single).get() === Seq(
        firstPipeline.apply(single).get(),
        secondPipeline.apply(single).get(),
        thirdPipeline.apply(single).get())
    }

    val data = Seq(13, 2, 83)
    val correctOut = data.map { x => Seq(
      firstPipeline.apply(x).get(),
      secondPipeline.apply(x).get(),
      thirdPipeline.apply(x).get())
    }
    assert(pipeline(sc.parallelize(data)).get().collect().toSeq === correctOut)
    assert(numEstimations.get() === 2, "Estimators should not have been fit again")

    // Clean global pipeline state
    Pipeline.state.clear()
  }
}
