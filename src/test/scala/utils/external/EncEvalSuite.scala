package utils.external

import java.io.File

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import nodes.learning.GaussianMixtureModel
import nodes.learning.external.GaussianMixtureModelEstimator
import org.scalatest.FunSuite
import pipelines.Logging
import utils.{Stats, TestUtils}

class EncEvalSuite extends FunSuite with Logging {

  test("Load SIFT Descriptors and compute Fisher Vector Features") {

    val siftDescriptor = csvread(new File(TestUtils.getTestResourceFileName("images/feats.csv")))

    val gmmMeans = TestUtils.getTestResourceFileName("images/voc_codebook/means.csv")
    val gmmVars = TestUtils.getTestResourceFileName("images/voc_codebook/variances.csv")
    val gmmWeights = TestUtils.getTestResourceFileName("images/voc_codebook/priors")

    val gmm = GaussianMixtureModel.load(gmmMeans, gmmVars, gmmWeights)

    val nCenters = gmm.means.cols
    val nDim = gmm.means.rows

    val extLib = new EncEval

    val fisherVector = extLib.calcAndGetFVs(
      gmm.means.toArray.map(_.toFloat),
      nCenters,
      nDim,
      gmm.variances.toArray.map(_.toFloat),
      gmm.weights.toArray.map(_.toFloat),
      siftDescriptor.toArray.map(_.toFloat))

    log.info(s"Fisher Vector is ${fisherVector.sum}")
    assert(Stats.aboutEq(fisherVector.sum, 40.109097, 1e-4), "SUM of Fisher Vectors must match expected sum.")

  }

  test("Compute a GMM from scala") {
    val nsamps = 10000

    // Generate two gaussians.
    val x = Gaussian(-1.0, 0.5).samples.take(nsamps).toArray
    val y = Gaussian(5.0, 1.0).samples.take(nsamps).toArray

    val z = shuffle(x ++ y).map(x => DenseVector(x))

    // Compute a 1-d GMM.
    val extLib = new EncEval
    val gmm = new GaussianMixtureModelEstimator(2).fit(z)

    logInfo(s"GMM means: ${gmm.means.toArray.mkString(",")}")
    logInfo(s"GMM vars: ${gmm.variances.toArray.mkString(",")}")
    logInfo(s"GMM weights: ${gmm.weights.toArray.mkString(",")}")

    // The results should be close to the distribution we set up.
    assert(Stats.aboutEq(min(gmm.means), -1.0, 1e-1), "Smallest mean should be close to -1.0")
    assert(Stats.aboutEq(max(gmm.means), 5.0, 1e-1), "Largest mean should be close to 1.0")
    assert(Stats.aboutEq(math.sqrt(min(gmm.variances)), 0.5, 1e-1), "Smallest SD should be close to 0.25")
    assert(Stats.aboutEq(math.sqrt(max(gmm.variances)), 1.0, 1e-1), "Largest SD should be close to 5.0")
  }
}