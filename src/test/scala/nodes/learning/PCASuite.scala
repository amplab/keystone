package nodes.learning

import breeze.stats.distributions.Gaussian
import breeze.linalg._
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.{Stats, MatrixUtils}

class PCATransformerSuite extends FunSuite with LocalSparkContext with Logging {

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


    //Validate matTwo's PCA transform
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

    //Generate a random matrix.
    val gau = new Gaussian(0.0, 1.0)
    val randMatrix = new DenseMatrix(matRows, matCols, (0 until matRows*matCols).toArray.map(_.toDouble)) +
      new DenseMatrix(matRows, matCols, gau.sample(matRows*matCols).toArray)

    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix).map(x => convert(x, Float)))

    val pca = new PCAEstimator(dimRed).fit(data)

    val pcacov = cov(convert(pca.pcaMat, Double))

    for (
      x <- 0 until dimRed;
      y <- 0 until dimRed if x != y
    ) {
      assert(Stats.aboutEq(pcacov(x,y), 0.0, 1e-4), s"PCA Matrix should be 0 off-diaganol. $x,$y = ${pcacov(x,y)}")
    }



  }
}
