package workflow

import breeze.linalg.DenseVector
import nodes.util.Identity
import org.apache.spark.rdd.RDD

case class ConcatTransformer[A] private[workflow] (branches: Seq[Seq[Transformer[_, _]]]) extends Transformer[A, DenseVector[Double]] {
  require(branches.nonEmpty, "Concat must have at least one branch")

  def apply(in: A): DenseVector[Double] = {
    DenseVector.vertcat(branches.map(branch => {
      // Unsafely apply the branch
      PipelineModel[A, DenseVector[Double]](branch).unsafeSingleApply(in)
    }): _*)
  }
  
  // Rewrites all the internal transformers, todo: flatten any nested Concats?
  private def flattenedBranches: Seq[Seq[Transformer[_, _]]] = {
    branches.map(_.flatMap(_.rewrite))
  }

  override def rewrite: Seq[Transformer[_, _]] = {
    // First rewrite the branches
    flattenedBranches match {
      // In the case of only one branch, no need for a Concat
      case Seq(onlyBranch) => onlyBranch

      // If all the branches start with the same prefix, it can be moved out of the Concat
      case ((prefix: Transformer[a, b]) +: firstBranchTail) +: otherBranches
        if otherBranches.forall(_.headOption == Some(prefix)) => {
        prefix +: ConcatTransformer(firstBranchTail +: otherBranches.map(_.tail)).rewrite
      }

      // Otherwise, just return a Concat with the rewritten branches
      case `branches` => Seq(ConcatTransformer(`branches`))
    }
  }
}

case class ConcatNode[A] private[workflow] (branches: Seq[Seq[Node[_, _]]]) extends Node[A, DenseVector[Double]] {
  require(branches.nonEmpty, "Concat must have at least one branch")

  // Rewrites all the internal transformers, todo: flatten any nested Concats?
  private def flattenedBranches: Seq[Seq[Node[_, _]]] = {
    branches.map(_.flatMap(_.rewrite))
  }

  def rewrite: Seq[Node[_, _]] = {
    // First rewrite the branches
    flattenedBranches match {
      // In the case of only one branch, no need for a Concat
      case Seq(onlyBranch) => onlyBranch

      // If all the branches start with the same prefix, it can be moved out of the Concat
      case ((prefix: Node[a, b]) +: firstBranchTail) +: otherBranches
        if otherBranches.forall(_.headOption == Some(prefix)) => {
        prefix +: ConcatNode(firstBranchTail +: otherBranches.map(_.tail)).rewrite
      }

      // If any pipeline is nested as the first node inside any branch,
      // flatten it and wrap the whole Concat in a pipeline
      case `branches` if `branches`.exists {
        case Pipeline(head) +: tail => true
        case _ => false
      } => {
        val rewrittenBranches = `branches`.map {
          case Pipeline(head) +: tail => head ++ tail
          case branch => branch
        }
        Seq(Pipeline(ConcatNode(rewrittenBranches).rewrite))
      }

      // Otherwise, just return a Concat with the rewritten branches
      case `branches` => Seq(ConcatNode(`branches`))
    }

  }

  def canElevate: Boolean = branches.forall(_.forall(_.canElevate))
}
