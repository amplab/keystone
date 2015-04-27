package pipelines.text

import breeze.linalg.{Vector, argmax}
import evaluation.MulticlassClassifierEvaluator
import loaders.NewsgroupsDataLoader
import nodes.learning.NaiveBayesEstimator
import nodes.misc.CommonSparseFeatures
import nodes.nlp._
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import utils.MatrixUtils

object NewsgroupsPipeline {

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
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val logger = LoggerFactory.getLogger(NewsgroupsPipeline.getClass)

    val newsgroupsData = NewsgroupsDataLoader(sc, trainingDir, testingDir)
    val numClasses = newsgroupsData.classes.length

    // Build the classifier estimator
    logger.info("Training classifier")
    val predictor = Trim.then(LowerCase())
        .then(Tokenizer()).then(new NGramsFeaturizer(1 to 2)).to[Seq[Any]].then(TermFrequency(x => 1))
        .thenEstimator(CommonSparseFeatures(100000)).fit(newsgroupsData.train.data).to[Vector[Double]]
        .thenLabelEstimator(NaiveBayesEstimator(numClasses))
        .fit(newsgroupsData.train.data, newsgroupsData.train.labels).thenMap(x => argmax(x))

    // Evaluate the classifier
    logger.info("Evaluating classifier")
    val testLabels = newsgroupsData.test.labels
    val testResults = predictor(newsgroupsData.test.data)
    val eval = MulticlassClassifierEvaluator(testResults, testLabels, numClasses)
    sc.stop()

    //MatrixUtils.pprint(confusionMatrix, newsgroupsData.classes, newsgroupsData.classes)
    logger.info("\n" + eval.pprintConfusionMatrix(newsgroupsData.classes))
    logger.info("Macro Precision: " + eval.macroPrecision)
    logger.info("Macro Recall: " + eval.macroRecall)
    logger.info("Macro F1: " + eval.macroFScore())
    logger.info("Micro Precision: " + eval.microPrecision)
    logger.info("Micro Recall: " + eval.microRecall)
    logger.info("Micro F1: " + eval.microFScore())
  }

}
