package pipelines.text

import breeze.linalg.{Vector, argmax}
import evaluation.MulticlassClassifierEvaluator
import loaders.NewsgroupsDataLoader
import nodes.learning.NaiveBayesEstimator
import nodes.misc.{TermFrequency, CommonSparseFeatures}
import nodes.nlp._
import nodes.util.MaxClassifier
import org.apache.spark.SparkContext
import pipelines.Logging

object NewsgroupsPipeline extends Logging {

  def main(args : Array[String]) {
    if (args.length < 4) {
      println("Usage: NewsgroupsPipeline <master> <sparkHome> <trainingDir> <testingDir>")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val sparkHome = args(1)
    val trainingDir = args(2)
    val testingDir = args(3)

    // Set up all the contexts
    val sc = new SparkContext(sparkMaster, "NewsgroupsPipeline", sparkHome, SparkContext.jarOfObject(this).toSeq)

    val newsgroupsData = NewsgroupsDataLoader(sc, trainingDir, testingDir)
    val numClasses = newsgroupsData.classes.length

    // Build the classifier estimator
    logInfo("Training classifier")
    val predictor = Trim.then(LowerCase())
        .then(Tokenizer()).then(new NGramsFeaturizer[String](1 to 2)).to[Seq[Any]].then(TermFrequency(x => 1))
        .thenEstimator(CommonSparseFeatures(100000)).fit(newsgroupsData.train.data).to[Vector[Double]]
        .thenLabelEstimator(NaiveBayesEstimator(numClasses))
        .fit(newsgroupsData.train.data, newsgroupsData.train.labels).then(MaxClassifier)

    // Evaluate the classifier
    logInfo("Evaluating classifier")
    val testLabels = newsgroupsData.test.labels
    val testResults = predictor(newsgroupsData.test.data)
    val eval = MulticlassClassifierEvaluator(testResults, testLabels, numClasses)
    sc.stop()

    logInfo("\n" + eval.summary(newsgroupsData.classes))
  }

}
