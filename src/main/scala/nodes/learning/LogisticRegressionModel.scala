package nodes.learning

import breeze.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionModel => MLlibLRM}
import org.apache.spark.mllib.linalg.{Vector => MLlibVector}
import org.apache.spark.mllib.optimization.{SquaredL2Updater, LogisticGradient, LBFGS}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, LabeledPoint}
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.rdd.RDD
import utils.MLlibUtils.breezeVectorToMLlib
import workflow.{LabelEstimator, Transformer}

import scala.reflect.ClassTag

/**
 * A Logistic Regression model that transforms feature vectors to vectors containing
 * the logistic regression output of the different classes
 */
class LogisticRegressionModel[T <: Vector[Double]](val model: MLlibLRM)
    extends Transformer[T, Double] {

  /**
   * Transforms a feature vector to a vector containing
   * the logistic regression output of the different classes

   * @param in The input feature vector
   * @return Logistic regression output of the classes for the input features
   */
  override def apply(in: T): Double = {
    model.predict(breezeVectorToMLlib(in))
  }
}

/**
 * A LabelEstimator which learns a Logistic Regression model from training data.
 * Currently does so using LBFG-S
 *
 * @param numClasses The number of classes
 * @param numIters The max number of iterations to use. Default 100
 * @param convergenceTol Set the convergence tolerance of iterations for the optimizer. Default 1E-4.
 */
case class LogisticRegressionEstimator[T <: Vector[Double] : ClassTag](
    numClasses: Int,
    numIters: Int = 100,
    convergenceTol: Double = 1E-4,
    numFeatures: Int = -1
  ) extends LabelEstimator[T, Double, Int] {

  /**
   * Train a classification model for Multinomial/Binary Logistic Regression using
   * Limited-memory BFGS. Standard feature scaling and L2 regularization are used by default.
   * NOTE: Labels used in Logistic Regression should be {0, 1, ..., k - 1}
   * for k classes multi-label classification problem.
   */
  private[this] class LogisticRegressionWithLBFGS(numClasses: Int, numFeaturesValue: Int)
      extends GeneralizedLinearAlgorithm[MLlibLRM] with Serializable {

    this.numFeatures = numFeaturesValue
    override val optimizer = new LBFGS(new LogisticGradient, new SquaredL2Updater)

    override protected val validators = List(multiLabelValidator)

    require(numClasses > 1)
    numOfLinearPredictor = numClasses - 1
    if (numClasses > 2) {
      optimizer.setGradient(new LogisticGradient(numClasses))
    }

    private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
      if (numOfLinearPredictor > 1) {
        DataValidators.multiLabelValidator(numOfLinearPredictor + 1)(data)
      } else {
        DataValidators.binaryLabelValidator(data)
      }
    }

    override protected def createModel(weights: MLlibVector, intercept: Double) = {
      if (numOfLinearPredictor == 1) {
        new MLlibLRM(weights, intercept)
      } else {
        new MLlibLRM(weights, intercept, numFeatures, numOfLinearPredictor + 1)
      }
    }
  }

  override def fit(in: RDD[T], labels: RDD[Int]): LogisticRegressionModel[T] = {
    val labeledPoints = labels.zip(in).map(x => LabeledPoint(x._1, breezeVectorToMLlib(x._2)))
    val trainer = new LogisticRegressionWithLBFGS(numClasses, numFeatures)
    trainer.setValidateData(false).optimizer.setNumIterations(numIters)
    val model = trainer.run(labeledPoints)

    new LogisticRegressionModel(model)
  }
}