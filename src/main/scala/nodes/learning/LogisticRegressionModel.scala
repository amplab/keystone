package nodes.learning

import breeze.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionModel => MLlibLRM, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import utils.MLlibUtils.breezeVectorToMLlib
import workflow.{LabelEstimator, Transformer}

import scala.reflect.ClassTag

/**
 * A Logistic Regression model that transforms feature vectors to vectors containing
 * the logistic regression values of the different classes
 */
class LogisticRegressionModel[T <: Vector[Double]](val model: MLlibLRM)
    extends Transformer[T, Double] {

  /**
   * Transforms a feature vector to a vector containing the log(posterior probabilities) of the different classes
   * according to this naive bayes model.

   * @param in The input feature vector
   * @return Log-posterior probabilites of the classes for the input features
   */
  override def apply(in: T): Double = {
    model.predict(breezeVectorToMLlib(in))
  }
}

/**
 * A LabelEstimator which learns a multinomial naive bayes model from training data.
 * Outputs a Transformer that maps features to vectors containing the log-posterior-probabilities
 * of the various classes according to the learned model.
 *
 * WARNING: This forcibly caches the data in a way the KeystoneML optimizer can't detect.
 * This is because of the native mllib code hiding stuff that allows disabling hardcoded caching.
 *
 * @param numClasses The number of classes
 * @param numIters The max number of iterations to use. Default 100
 * @param convergenceTol Set the convergence tolerance of iterations for L-BFGS. Default 1E-4.
 */
case class LogisticRegressionLBFGSEstimator[T <: Vector[Double] : ClassTag](numClasses: Int, numIters: Int = 100, convergenceTol: Double = 1E-4)
    extends LabelEstimator[T, Double, Int] {
  override def fit(in: RDD[T], labels: RDD[Int]): LogisticRegressionModel[T] = {
    val labeledPoints = labels.zip(in).map(x => LabeledPoint(x._1, breezeVectorToMLlib(x._2)))
    val trainer = new LogisticRegressionWithLBFGS().setNumClasses(numClasses)
    trainer.setValidateData(false).optimizer.setConvergenceTol(convergenceTol).setNumIterations(numIters)
    val model = trainer.run(labeledPoints.cache())

    new LogisticRegressionModel(model)
  }
}
