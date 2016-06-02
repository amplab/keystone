package nodes.stats

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.PipelineContext
import utils.{MLlibUtils, Stats}

class StandardScalerSuite extends FunSuite with PipelineContext {

  // When the input data is all constant, the variance is zero. The standardization against
  // zero variance is not well-defined, but we decide to just set it into zero here.
  val constantData = Array(
    DenseVector(2.0),
    DenseVector(2.0),
    DenseVector(2.0)
  )

  val denseData = Array(
    DenseVector(-2.0, 2.3, 0.0),
    DenseVector(0.0, -1.0, -3.0),
    DenseVector(0.0, -5.1, 0.0),
    DenseVector(3.8, 0.0, 1.9),
    DenseVector(1.7, -0.6, 0.0),
    DenseVector(0.0, 1.9, 0.0)
  )

  private def computeSummary(data: RDD[DenseVector[Double]]): MultivariateStatisticalSummary = {
    data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(MLlibUtils.breezeVectorToMLlib(data)),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
  }

  test("Standardization with dense input when means and stds are provided") {
    sc = new SparkContext("local", "test")
    val dataRDD = sc.parallelize(denseData, 3)

    val standardizer1 = new StandardScaler()

    val model1 = standardizer1.fit(dataRDD)

    val equivalentModel1 = new StandardScalerModel(model1.mean, model1.std)

    val data1 = denseData.map(equivalentModel1.apply)

    val data1RDD = equivalentModel1.apply(dataRDD)

    val summary = computeSummary(dataRDD)
    val summary1 = computeSummary(data1RDD)

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => Stats.aboutEq(v1, v2, 1E-5)))

    assert(Stats.aboutEq(MLlibUtils.mllibVectorToDenseBreeze(summary1.mean), DenseVector(0.0, 0.0, 0.0), 1E-5))
    assert(Stats.aboutEq(MLlibUtils.mllibVectorToDenseBreeze(summary1.variance), DenseVector(1.0, 1.0, 1.0), 1E-5))

    assert(Stats.aboutEq(data1(0), DenseVector(-1.31527964, 1.023470449, 0.11637768424), 1E-5))
    assert(Stats.aboutEq(data1(3), DenseVector(1.637735298, 0.156973995, 1.32247368462), 1E-5))
  }

  test("Standardization with dense input") {
    sc = new SparkContext("local", "test")
    val dataRDD = sc.parallelize(denseData, 3)

    val standardizer1 = new StandardScaler()

    val model1 = standardizer1.fit(dataRDD)

    val data1 = denseData.map(model1.apply)

    val data1RDD = model1.apply(dataRDD)

    val summary = computeSummary(dataRDD)
    val summary1 = computeSummary(data1RDD)

    assert((data1, data1RDD.collect()).zipped.forall((v1, v2) => Stats.aboutEq(v1, v2, 1E-5)))

    assert(Stats.aboutEq(MLlibUtils.mllibVectorToDenseBreeze(summary1.mean), DenseVector(0.0, 0.0, 0.0), 1E-5))
    assert(Stats.aboutEq(MLlibUtils.mllibVectorToDenseBreeze(summary1.variance), DenseVector(1.0, 1.0, 1.0), 1E-5))

    assert(Stats.aboutEq(data1(0), DenseVector(-1.31527964, 1.023470449, 0.11637768424), 1E-5))
    assert(Stats.aboutEq(data1(3), DenseVector(1.637735298, 0.156973995, 1.32247368462), 1E-5))
  }

  test("Standardization with constant input when means and stds are provided") {
    sc = new SparkContext("local", "test")

    val dataRDD = sc.parallelize(constantData, 2)
    val standardizer = new StandardScaler()
    val model = standardizer.fit(dataRDD)
    val equivalentModel = new StandardScalerModel(model.mean, model.std)
    val data = constantData.map(equivalentModel.apply)

    assert(data.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
  }

  test("Standardization with constant input") {
    sc = new SparkContext("local", "test")

    val dataRDD = sc.parallelize(constantData, 2)
    val standardizer = new StandardScaler()
    val model = standardizer.fit(dataRDD)
    val data = constantData.map(model.apply)

    assert(data.forall(_.toArray.forall(_ == 0.0)),
      "The variance is zero, so the transformed result should be 0.0")
  }
}