package evaluation

import org.apache.spark.rdd.RDD
import workflow.PipelineDataset

trait Evaluator[P,L,E] {
  def apply(predictions: RDD[P], labels: RDD[L]): E
  def apply(predictions: PipelineDataset[P], labels: RDD[L]): E = apply(predictions.get, labels)
  def apply(predictions: RDD[P], labels: PipelineDataset[L]): E = apply(predictions, labels.get)
  def apply(predictions: PipelineDataset[P], labels: PipelineDataset[L]): E = apply(predictions.get, labels.get)
}