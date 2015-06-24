package workflow

import org.apache.spark.rdd.RDD

/**
 * A transformer used internally to apply the fit output of an Estimator onto data dependencies.
 * Takes one fit dependency, and directly applies its transform onto the data dependencies.
 *
 * Only expects one fit dependency. This is because the DSL will place a new DelegatingTransformer
 * after each Estimator whenever chaining an Estimator, and connect it via a fit dependency.
 */
private[workflow] class DelegatingTransformer[T](override val label: String)
    extends TransformerNode[T] {

  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): T = {
    fitDependencies.head.transform(dataDependencies, Seq()).asInstanceOf[T]
  }

  def transformRDD(
    dataDependencies: Seq[RDD[_]],
    fitDependencies: Seq[TransformerNode[_]]): RDD[T] = {
    fitDependencies.head.transformRDD(dataDependencies, Seq()).asInstanceOf[RDD[T]]
  }
}