package evaluation

import org.apache.spark.rdd.RDD
import workflow.PipelineDataset

/**
  * An Evaluator is an object whose "evaluate" method takes a vector of Predictions and a set of Labels (of the same
  * length and order) and returns an "Evaluation" which is specific to the domain (binary classification, multi-label
  * classification, etc.). The Evaluation is typically a set of summary statistics designed to capture the performance
  * of a machine learning pipeline.
  *
  * Because evaluation typically happens at the end of a pipeline, we support the cartesian product of
  * {RDD, PipelineDataset} for both sets of arguments.
  *
  * @tparam P Type of Predictions.
  * @tparam L Type of the Labels.
  * @tparam E Type of the Evaluation.
  */
trait Evaluator[P,L,E] {

  /**
    * Generate an evaluation.
    *
    * @param predictions Predicted values.
    * @param labels True labels. (Same order and length and the predictions).
    *
    * @return An evaluation.
    */
  def evaluate(predictions: RDD[P], labels: RDD[L]): E

  def evaluate(predictions: PipelineDataset[P], labels: RDD[L]): E = evaluate(predictions.get, labels)

  def evaluate(predictions: RDD[P], labels: PipelineDataset[L]): E = evaluate(predictions, labels.get)
  
  def evaluate(predictions: PipelineDataset[P], labels: PipelineDataset[L]): E = evaluate(predictions.get, labels.get)
}