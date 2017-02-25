package keystoneml.nodes.learning

import breeze.linalg._
import breeze.stats.distributions.{Multinomial, Uniform, Gaussian}
import keystoneml.nodes.stats.StandardScaler
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import keystoneml.pipelines.Logging
import keystoneml.utils.{TestUtils, MatrixUtils, Stats}
import keystoneml.workflow.PipelineContext

class LinearDiscriminantAnalysisSuite extends FunSuite with PipelineContext with Logging {
  test("Solve Linear Discriminant Analysis on the Iris Dataset") {
    sc = new SparkContext("local", "test")

    // Uses the Iris flower dataset
    val irisData = sc.parallelize(TestUtils.loadFile("iris.data"))
    val trainData = irisData.map(_.split(",").dropRight(1).map(_.toDouble)).map(new DenseVector(_))
    val features = new StandardScaler().fit(trainData).apply(trainData)
    val labels = irisData.map(_ match {
      case x if x.endsWith("Iris-setosa") => 1
      case x if x.endsWith("Iris-versicolor") => 2
      case x if x.endsWith("Iris-virginica") => 3
    })

    val lda = new LinearDiscriminantAnalysis(2)
    val out = lda.fit(features, labels)

    // Correct output taken from http://sebastianraschka.com/Articles/2014_python_lda.html#introduction
    logInfo(s"\n${out.x}")
    val majorVector = DenseVector(-0.1498, -0.1482, 0.8511, 0.4808)
    val minorVector = DenseVector(0.0095, 0.3272, -0.5748, 0.75)

    // Note that because eigenvectors can be reversed and still valid, we allow either direction
    assert(Stats.aboutEq(out.x(::, 0), majorVector, 1E-4) || Stats.aboutEq(out.x(::, 0), majorVector * -1.0, 1E-4))
    assert(Stats.aboutEq(out.x(::, 1), minorVector, 1E-4) || Stats.aboutEq(out.x(::, 1), minorVector * -1.0, 1E-4))
  }

  test("Check LDA output for a diagonal covariance") {
    sc = new SparkContext("local", "test")

    val matRows = 1000
    val matCols = 10
    val dimRed = 5

    // Generate a random Gaussian matrix.
    val gau = new Gaussian(0.0, 1.0)
    val randMatrix = new DenseMatrix(matRows, matCols, gau.sample(matRows*matCols).toArray)

    // Parallelize and estimate the LDA.
    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix))
    val labels = data.map(x => Multinomial(DenseVector(0.2, 0.2, 0.2, 0.2, 0.2)).draw(): Int)
    val lda = new LinearDiscriminantAnalysis(dimRed).fit(data, labels)

    // Apply LDA to the input data.
    val redData = lda(data)
    val redMat = MatrixUtils.rowsToMatrix(redData.collect)

    // Compute its covariance.
    val redCov = cov(redMat)
    log.info(s"Covar\n$redCov")

    // The covariance of the dimensionality reduced matrix should be diagonal.
    for (
      x <- 0 until dimRed;
      y <- 0 until dimRed if x != y
    ) {
      assert(Stats.aboutEq(redCov(x,y), 0.0, 1e-6), s"LDA Matrix should be 0 off-diagonal. $x,$y = ${redCov(x,y)}")
    }
  }

}
