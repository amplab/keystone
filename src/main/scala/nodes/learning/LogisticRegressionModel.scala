package nodes.learning

import breeze.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionModel => MLlibLRM, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
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
    convergenceTol: Double = 1E-4
  ) extends LabelEstimator[T, Double, Int] {
  override def fit(in: RDD[T], labels: RDD[Int]): LogisticRegressionModel[T] = {
    val labeledPoints = labels.zip(in).map(x => LabeledPoint(x._1, breezeVectorToMLlib(x._2)))
    val trainer = new LogisticRegressionWithLBFGS().setNumClasses(numClasses)

    // Disable feature scaling using reflection
    val featureScalingMethod = trainer.getClass.getSuperclass.getDeclaredMethod("setFeatureScaling",
      classOf[Boolean])
    featureScalingMethod.setAccessible(true)
    featureScalingMethod.invoke(trainer, new java.lang.Boolean(false))

    trainer.setValidateData(false).optimizer.setConvergenceTol(convergenceTol).setNumIterations(numIters)
    val model = trainer.run(labeledPoints)
    new LogisticRegressionModel(model)
  }

}
