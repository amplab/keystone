package pipelines

import org.opencv.core._
import org.apache.spark.rdd.RDD
import pipelines.Pipelines.PipelineNode

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object Pipelines {
  type PipelineNode[Input, Output] = (Input => Output)
}

trait OpenCVUser {
  OpenCVUtils.init()
}

trait PipelineStage[A, B]

private case class UnsupervisedEstimatorWithData[A, B](e: UnsupervisedEstimator[A, B], data: RDD[A]) extends PipelineStage[A, B]
private case class SupervisedEstimatorWithData[I, O, L](e: SupervisedEstimator[I, O, L], data: RDD[I], labels: RDD[L]) extends PipelineStage[I, O]

class Pipeline[A : ClassTag, B : ClassTag, C : ClassTag, L] private (initStage: PipelineStage[A, B], nextStage: PipelineStage[B, C]) extends SupervisedEstimator[A, C, L] {

  def addStage[D : ClassTag](stage: Transformer[C, D]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  def addStage[D : ClassTag](stage: UnsupervisedEstimator[C, D]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  def addStage[D : ClassTag](stage: SupervisedEstimator[C, D, L]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, stage)
  }

  def addStage[D : ClassTag](stage: UnsupervisedEstimator[C, D], data: RDD[C]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, UnsupervisedEstimatorWithData(stage, data))
  }

  def addStage[D : ClassTag](stage: SupervisedEstimator[C, D, L], data: RDD[C], labels: RDD[L]): Pipeline[A, C, D, L] = {
    new Pipeline[A, C, D, L](this, SupervisedEstimatorWithData(stage, data, labels))
  }

  override def fit(in: RDD[A], labels: RDD[L]): Transformer[A, C] = {
    val firstTransformer = initStage match {
      case estimator: UnsupervisedEstimator[A, B] => estimator.fit(in)
      case estimator: SupervisedEstimator[A, B, L] => estimator.fit(in, labels)
      case SupervisedEstimatorWithData(x, data, l) => x.fit(data, l)
      case UnsupervisedEstimatorWithData(x, data) => x.fit(data)
      case transformer: Transformer[A, B] => transformer
      case _ => throw new IllegalArgumentException
    }

    val secondTransformer = nextStage match {
      case estimator: UnsupervisedEstimator[B, C] => estimator.fit(firstTransformer.transform(in))
      case estimator: SupervisedEstimator[B, C, L] => estimator.fit(firstTransformer.transform(in), labels)
      case SupervisedEstimatorWithData(x, data, l) => x.fit(data, l)
      case UnsupervisedEstimatorWithData(x, data) => x.fit(data)
      case transformer: Transformer[B, C] => transformer
      case _ => throw new IllegalArgumentException
    }

    new TransformerChain[A, B, C](firstTransformer, secondTransformer)
  }
}

object Pipeline {
  def apply[A : ClassTag, B : ClassTag, C : ClassTag, L](a: PipelineStage[A, B], b: PipelineStage[B, C]) = new Pipeline[A, B, C, L](a, b)
  def apply[A : ClassTag, B : ClassTag, L](a: PipelineStage[A, B]) = new Pipeline[A, A, B, L](null, a)
  def apply[A : ClassTag, L]() = new Pipeline[A, A, A, L](new IdentityTransformer[A], new IdentityTransformer[A])
}

private class TransformerChain[A : ClassTag, B, C : ClassTag](val a: Transformer[A, B], val b: Transformer[B, C]) extends Transformer[A, C] {
  override def transform(in: A): C = b.transform(a.transform(in))
  override def transform(in: RDD[A]): RDD[C] = b.transform(a.transform(in))
}

class IdentityTransformer[T : ClassTag] extends Transformer[T, T] {
  override def transform(in: T): T = in
  override def transform(in: RDD[T]): RDD[T] = in
}

abstract class Transformer[A : ClassTag, B : ClassTag] extends PipelineNode[RDD[A], RDD[B]] with Serializable with PipelineStage[A, B] {
  def transform(in: A): B
  def transform(in: RDD[A]): RDD[B] = in.map(transform)
  def apply(in: RDD[A]): RDD[B] = transform(in)
  def andThen[C : ClassTag](next: Transformer[B, C]): Transformer[A, C] = new TransformerChain[A, B, C](this, next)
}

abstract class UnsupervisedEstimator[A, B] extends Serializable with PipelineStage[A, B] {
  def fit(in: RDD[A]): Transformer[A, B]
}

abstract class SupervisedEstimator[I, O, L] extends Serializable with PipelineStage[I, O] {
  def fit(data: RDD[I], labels: RDD[L]): Transformer[I, O]
}

object OpenCVUtils {
  def init() { System.loadLibrary(Core.NATIVE_LIBRARY_NAME) }
}