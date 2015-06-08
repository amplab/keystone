package workflow

import scala.reflect.ClassTag

case class ScatterTransformer[A, B : ClassTag] private[workflow] (branches: Seq[Seq[Transformer[_, _]]]) extends Transformer[A, Seq[B]] {
  require(branches.nonEmpty, "Concat must have at least one branch")

  def apply(in: A): Seq[B] = {
    branches.map(branch => {
      PipelineModel[A, B](branch).apply(in)
    })
  }
  
  // Rewrites all the internal transformers
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
        prefix +: ScatterTransformer(firstBranchTail +: otherBranches.map(_.tail)).rewrite
      }

      // Otherwise, just return a Concat with the rewritten branches
      case `branches` => Seq(ScatterTransformer(`branches`))
    }
  }
}

case class ScatterNode[A, B] private[workflow] (branches: Seq[Seq[Node[_, _]]]) extends Node[A, Seq[B]] {
  require(branches.nonEmpty, "Concat must have at least one branch")

  // Rewrites all the internal transformers
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
        prefix +: ScatterNode(firstBranchTail +: otherBranches.map(_.tail)).rewrite
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
        Seq(Pipeline(ScatterNode(rewrittenBranches).rewrite))
      }

      // Otherwise, just return a Concat with the rewritten branches
      case `branches` => Seq(ScatterNode(`branches`))
    }

  }

  def canSafelyPrependExtraNodes: Boolean = branches.forall(_.forall(_.canSafelyPrependExtraNodes))
}

object Scatter {
  def apply[A, B : ClassTag](branches: Seq[Transformer[A, B]]): Transformer[A, Seq[B]] = {
    PipelineModel(ScatterTransformer[A, B](branches.map(_.rewrite)).rewrite)
  }

  def apply[A, B : ClassTag](branches: Seq[Node[A, B]]): Pipeline[A, Seq[B]] = {
    ScatterNode[A, B](branches.map(_.rewrite))
  }
}
