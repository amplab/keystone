package nodes.learning

import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Gaussian}
import breeze.stats.mean
import breeze.linalg._
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.{TestUtils, Stats, MatrixUtils}

class PCASuite extends FunSuite with LocalSparkContext with Logging {

  test("PCA matrix transformation") {
    sc = new SparkContext("local", "test")

    val matOneHeight = 3
    val matTwoHeight = 8
    val matDims = 4
    val outDims = 2

    val pcaMatrix = new DenseMatrix(matDims, outDims, Array[Float](1, 3, 5, 7, 2, 4, 6, 8))
    val pcaTransformer = new PCATransformer(pcaMatrix)

    val matOne = new DenseMatrix(matOneHeight, matDims, (0 until matOneHeight*matDims).toArray.map(_.toDouble))
    val matOneRows = convert(MatrixUtils.matrixToRowArray(matOne), Float)

    val matTwo = new DenseMatrix(matTwoHeight, matDims, Array.fill[Double](matTwoHeight * matDims)(1))
    val matTwoRows = convert(MatrixUtils.matrixToRowArray(matTwo), Float)

    val out = pcaTransformer.apply(sc.parallelize(matOneRows ++ matTwoRows)).collect()

    // Validate matOne's PCA transform
    val outOne = out.slice(0, matOneHeight).toArray

    assert(outOne(0)(0) == 102.0f, "(0,0) of Matrix one should be 102")
    assert(outOne(0)(1) == 120.0f, "(0,0) of Matrix one should be 120")
    assert(outOne(1)(0) == 118.0f, "(0,0) of Matrix one should be 118")
    assert(outOne(1)(1) == 140.0f, "(0,0) of Matrix one should be 140")
    assert(outOne(2)(0) == 134.0f, "(0,0) of Matrix one should be 134")
    assert(outOne(2)(1) == 160.0f, "(0,0) of Matrix one should be 160")


    // Validate matTwo's PCA transform
    val outTwo = out.slice(matOneHeight, out.length).toArray

    (0 until matTwoHeight).foreach(x => {
      assert(outTwo(x)(0) == 16 && outTwo(x)(1) == 20, "All of matTwo should be (16,20)")
    })
  }


  test("PCA Estimation") {
    sc = new SparkContext("local", "test")

    val matRows = 1000
    val matCols = 10
    val dimRed = 5

    // Generate a random Gaussian matrix.
    val randMatrix = TestUtils.createLocalRandomMatrix(matRows, matCols)

    // Parallelize and estimate the PCA.
    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix).map(x => convert(x, Float)))
    val pca = new PCAEstimator(dimRed).fit(data)

    // Apply PCA to the input data.
    val redData = pca(data)
    val redMat = MatrixUtils.rowsToMatrix(redData.collect.map(x => convert(x, Double)))

    // Compute its covariance.
    val redCov = cov(redMat)
    logDebug(s"Covar$redCov")

    // The covariance of the dimensionality reduced matrix should be diagonal.
    for (
      x <- 0 until dimRed;
      y <- 0 until dimRed if x != y
    ) {
      assert(Stats.aboutEq(redCov(x,y), 0.0, 1e-4), s"PCA Matrix should be 0 off-diagonal. $x,$y = ${redCov(x,y)}")
    }
  }

  test("Covariance Matrix of Distributed PCA should match local one") {
    sc = new SparkContext("local", "test")

    val matRows = 1000
    val matCols = 10
    val dimRed = 5

    // Generate a random Gaussian matrix.
    val randMatrix = TestUtils.createLocalRandomMatrix(matRows, matCols)

    // Parallelize and estimate the PCA.
    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix).map(x => convert(x, Float)), 4)

    val pcaDist = new DistributedPCAEstimator(dimRed).fit(data)
    val pcaLocal = new PCAEstimator(dimRed).fit(data)


    assert(Stats.aboutEq(convert(pcaDist.pcaMat, Double), convert(pcaLocal.pcaMat, Double), 1e-4))
  }

  /**
    * Generates a noisy low-rank matrix.
    *
    * Result is A*B + E,
    *
    * Where
    * A is Gaussian(0,1) \in R^{n \times k}
    * B is Gaussian(0,1) \in R^{k \times d}
    * E is Gaussian(0,eps) in R^{n \times d}
    *
    * @param n Number of rows.
    * @param d Number of columns.
    * @param k Rank of factors.
    * @param eps Variance of the Gaussian noise.
    * @return A noisy low-rank matrix.
    */
  def lowRank(n: Int, d: Int, k: Int, eps: Double = 1e-4): DenseMatrix[Double] = {
    val gau = new Gaussian(0.0, 1.0)

    val leftPart = new DenseMatrix(n, k, gau.sample(n*k).toArray)
    val rightPart = new DenseMatrix(k, d, gau.sample(k*d).toArray)

    val noisyGau = new Gaussian(0.0, eps)
    val noise = new DenseMatrix(n, d, noisyGau.sample(n*d).toArray)

    leftPart*rightPart + noise
  }

  test("Sketch algorithm should produce a valid sketch of the matrix") {
    val matRows = 200
    val matCols = 100
    val dimRed = 10

    // Generate a random Gaussian matrix.
    val randMatrix = TestUtils.createLocalRandomMatrix(matRows, matCols)
    logInfo(s"${randMatrix(0 to 5, 0 to 5)}")

    for (
      p <- 5 to 10;
      k <- List(1, 5, 10, 20);
      q <- 1 to 20
    ) {
      logDebug(s"Starting ${k + p}, $q")
      val Q = ApproximatePCAEstimator.approximateQ(randMatrix, k + p, q)
      logDebug(s"Got q: ${Q(0 to 3, 0 to 3)}")
      logDebug(s"Got randMatrix: ${randMatrix(0 to 3, 0 to 3)}")
      logDebug(s"Got diff: ${(randMatrix - (Q * Q.t * randMatrix)).apply(0 to 3, 0 to 3)}")
      val err = randMatrix - (Q * Q.t * randMatrix)
      val eps = norm(err.toDenseVector)

      //From 1.9 of HMT2011
      assert(eps < (1 + 9 * math.sqrt(k + p) * math.min(matRows, matCols)) * svd(randMatrix).S(k))
    }

  }

  test("Singular values of low-rank projection should be similar regardless of method used.") {
    sc = new SparkContext("local", "test")

    val matRows = 200
    val matCols = 100
    val dimRed = 10

    // Generate a random Gaussian matrix.
    val randMatrix = TestUtils.createLocalRandomMatrix(matRows, matCols)

    // Parallelize and estimate the PCA.
    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix).map(x => convert(x, Float)))

    val pcaApprox = new ApproximatePCAEstimator(dimRed, q = 10).fit(data)
    val pcaLocal = new PCAEstimator(dimRed).fit(data)

    val approxDimred = pcaApprox.apply(data)
    val dimred = pcaLocal.apply(data)

    val approxDimredMat = MatrixUtils.rowsToMatrix(approxDimred.collect)
    val dimredMat = MatrixUtils.rowsToMatrix(dimred.collect)


    val approxSingularValues = svd(approxDimredMat).S
    val exactSingularValues = svd(dimredMat).S

    logDebug(s"Singular values of approx: $approxSingularValues")
    logDebug(s"Singular values of exact: $exactSingularValues")

    def mre(approx: DenseVector[Float], base: DenseVector[Float]): Float = mean(abs(approx-base)/abs(base))

    //These should be off by less than one percent.
    logDebug(s"mre: ${mre(approxSingularValues, exactSingularValues)}")
    assert(mre(approxSingularValues, exactSingularValues) < 0.05)
  }

  test("Approximate PCA application should result in a matrix that's basically diagonal covariance.") {
    sc = new SparkContext("local", "test")

    val matRows = 200
    val matCols = 100
    val dimRed = 10

    // Generate a random Gaussian matrix.
    val randMatrix = TestUtils.createLocalRandomMatrix(matRows, matCols)

    // Parallelize and estimate the PCA.
    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix).map(x => convert(x, Float)))

    val pcaApprox = new ApproximatePCAEstimator(dimRed, q = 10).fit(data)
    val pcaLocal = new PCAEstimator(dimRed).fit(data)

    val approxDimred = pcaApprox.apply(data)
    val dimred = pcaLocal.apply(data)

    val approxDimredMat = MatrixUtils.rowsToMatrix(approxDimred.collect)
    val dimredMat = MatrixUtils.rowsToMatrix(dimred.collect)

    val cadm = cov(convert(approxDimredMat, Double))
    val offDiagCadm = cadm - (DenseMatrix.eye[Double](cadm.rows) :* cadm)

    logDebug(s"offDiagCadm: $offDiagCadm")
    logDebug(s"maximum off-diagonal covariance ${max(offDiagCadm)}")

    assert(Stats.aboutEq(offDiagCadm, DenseMatrix.zeros[Double](cadm.rows, cadm.rows), 0.1))
  }
}
