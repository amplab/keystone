package evaluation

import org.apache.spark.rdd.RDD
import workflow.PipelineDataset

trait Evaluator[P,L,E] {
  def evaluate(predictions: RDD[P], labels: RDD[L]): E
  def evaluate(predictions: PipelineDataset[P], labels: RDD[L]): E = evaluate(predictions.get, labels)
  def evaluate(predictions: RDD[P], labels: PipelineDataset[L]): E = evaluate(predictions, labels.get)
  def evaluate(predictions: PipelineDataset[P], labels: PipelineDataset[L]): E = evaluate(predictions.get, labels.get)
}