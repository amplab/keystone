package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import nodes.stats.StandardScaler
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{MatrixUtils, Stats, KernelUtils}

class KernelModelSuite extends FunSuite with LocalSparkContext with Logging {

  test("KernelModel XOR test") {
    sc = new SparkContext("local", "test")
    val gaussian = new GaussianKernelGenerator(10)
    val clf = new KernelRidgeRegression(gaussian,  Array(0), 2, 2)

    val x:Array[DenseVector[Double]] = Array(DenseVector(-1.0,-1.0),DenseVector(1.0,1.0),DenseVector(-1,1),DenseVector(1,-1))
    val xTest:Array[DenseVector[Double]] = Array(DenseVector(-1.0,-1.0),DenseVector(1.0,1.0),DenseVector(-1,1))
    val y:Array[DenseVector[Double]] = Array(DenseVector(0.0,1.0), DenseVector(0.0,1.0), DenseVector(1.0,0.0), DenseVector(1.0,0.0))
    val yTest:Array[DenseVector[Double]] = Array(DenseVector(0.0,1.0), DenseVector(0.0,1.0), DenseVector(1.0,0.0))

    val xRDD = sc.parallelize(x,2)
    val yRDD = sc.parallelize(y,2)
    val xTestRDD = sc.parallelize(xTest,2)
    val kernelModel = clf.fit(xRDD, yRDD)
    val yHat:Array[DenseVector[Double]] = kernelModel(xTestRDD).collect().map(_(0))
    /* Fit should be good */
    val delta = KernelUtils.rowsToMatrix(yHat) - KernelUtils.rowsToMatrix(yTest)


    delta :*= delta
    assert(Stats.aboutEq(sum(delta), 0, 1e-4))

  }

}
