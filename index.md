---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# KeystoneML
[![Build Status](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/badge/icon)](https://amplab.cs.berkeley.edu/jenkins/job/KeystoneML/)

KeystoneML is a software framework, written in [Scala](http://scala-lang.org/), from the [UC Berkeley AMPLab](http://amplab.cs.berkeley.edu/) designed to simplify the construction of *large scale*, *end-to-end*, machine learning pipelines with [Apache Spark](http://spark.apache.org/).

We contributed to the design of [spark.ml](https://spark.apache.org/docs/latest/ml-guide.html) during the development of KeystoneML, so if you're familiar with `spark.ml` then you'll recognize some shared concepts, but there are a few important differences, particularly around type safety and chaining, which lead to pipelines that are easier to construct and more robust.

KeystoneML also presents a richer set of operators than those present in `spark.ml` including featurizers for images, text, and speech, and provides several example pipelines that reproduce state-of-the-art academic results on public data sets.


## What is KeystoneML for?
KeystoneML makes constructing even complicated machine learning pipelines easy. Here's an example text categorization pipeline which creates bigram features and creates a Naive Bayes model based on the 100,000 most common features.

```scala
val trainData = NewsGroupsDataLoader(sc, trainingDir)

val predictor = Trim andThen
    LowerCase() andThen
    Tokenizer() andThen
    NGramsFeaturizer(1 to conf.nGrams) andThen
    TermFrequency(x => 1) andThen
    (CommonSparseFeatures(conf.commonFeatures), trainData.data) andThen
    (NaiveBayesEstimator(numClasses), trainData.data, trainData.labels) andThen
    MaxClassifier
```

Parallelization of the pipeline fitting process is handled automatically and pipeline nodes are designed to scale horizontally.

Once the pipeline has been defined you can apply it to test data and evaluate its effectiveness.

```scala
val test = NewsGroupsDataLoader(sc, testingDir)
val predictions = predictor(test.data)
val eval = MulticlassClassifierEvaluator(predictions, test.labels, numClasses)

println(eval.summary(newsgroupsData.classes))
```

The result of this code will contain the following:

```
Avg Accuracy:	0.980
Macro Precision:0.816
Macro Recall:	0.797
Macro F1:	0.797
Total Accuracy:	0.804
Micro Precision:0.804
Micro Recall:	0.804
Micro F1:	0.804
```

This relatively simple pipeline predicts the right document category over 80% of the time on the test set.

Of course, you can the pipeline in another system on new samples of text - just like any other function.

```scala
println(newsgroupsData.classes(predictor("The Philadelphia Phillies win the World Series!")))
```

Which prints the following:

```
rec.sport.baseball
```

KeystoneML works with much more than just text. Have a look at our [examples](examples.html) to see pipelines in the domains of computer vision and speech.

KeystoneML is alpha software, in a very early public release (v0.2).
The project is still very young, but we feel that it has grown to the point where it is viable for general use.

## News
* 2016-03-24 KeystoneML version 0.3.0 has been released and pushed to Maven central. See [the release notes](release.html) for more information.
* 2015-10-08 We've put together a [minimal example application](https://github.com/amplab/keystone-example) for you to use as a basis for starting your own projects that use KeystoneML.
* 2015-09-18 KeystoneML version 0.2.0 has been pushed to Maven central. See [the release notes](release.html) for more information.
* 2015-09-17 KeystoneML is on Maven Central. We have added a new "linking" section below.

## Linking

KeystoneML is available from Maven Central. You can use it in your applications by adding the following lines to your SBT project definition:

```scala
libraryDependencies += "edu.berkeley.cs.amplab" % "keystoneml_2.10" % "0.3.0"
```

[See here](https://github.com/amplab/keystone-example) for an example application which uses KeystoneML (and has scripts for launching a cluster configured with KeystoneML

## Developing

KeystoneML is available on [GitHub](http://github.com/amplab/keystone/). 

```
$ git clone https://github.com/amplab/keystone.git
```


## Building

Once downloaded, you can build KeystoneML with the following commands:

```
$ cd keystone
$ git checkout branch-v0.3
$ sbt/sbt assembly
$ make
```

This will automatically resolve dependencies and package a jar file in `target/keystone/scala-2.10/keystone-assembly-0.3.jar`.

You can then run example pipelines with the included `bin/run-pipeline.sh` script, or pass as an argument to `spark-submit`.


## Running an Example

Once you've built KeystoneML, you can run many of the example pipelines locally.
However, to run the larger examples, you'll want access to a Spark cluster.

Here's an example of running a handwriting recognition pipeline on the popular MNIST dataset. 
You should be able to run this on a single machine in under a minute.

```
#Get the data from S3
wget http://mnist-data.s3.amazonaws.com/train-mnist-dense-with-labels.data
wget http://mnist-data.s3.amazonaws.com/test-mnist-dense-with-labels.data

KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
```

To run on a cluster, we recommend using the `spark-ec2` to launch a cluster and provision with correct versions of [BLAS](http://www.netlib.org/blas/) and native C libraries used by KeystoneML.

We've provided some scripts to set up a well-configured cluster automatically in `bin/pipelines-ec2.sh`. You can read more about using them [here](running_pipelines.html).

## Building New Pipelines

Now that you've seen an example pipeline, have a look at the [programming guide](programming_guide.html). 

After that, head over to the [API documentation](api/latest/).

## People
KeystoneML is under active development in the UC Berkeley AMPLab. Development is led by Evan Sparks, Shivaram Venkataraman, Tomer Kaftan, Michael Franklin and Benjamin Recht. 

For more information please contact <a href="mailto:sparks@cs.berkeley.edu,shivaram@cs.berkeley.edu?subject=KeystoneML">Evan Sparks and Shivaram Venkataraman</a>.

## Getting Help and Contributing
For help using the software please join and send mail to the [KeystoneML users list](https://groups.google.com/forum/#!forum/keystoneml-users).

KeystoneML is an Apache Licensed open-source project and we welcome contributions.
Have a look at our [Github Issues page](http://github.com/amplab/keystone/issues) if you'd like to contribute, and feel free to fork the repo and submit a pull request!

## Acknowledgements
Research on KeystoneML is a part of the [AMPLab at UC Berkeley](http://amplab.cs.berkeley.edu/). This research is supported in part by NSF CISE Expeditions Award CCF-1139158, DOE Award SN10040 DE-SC0012463, and DARPA XData Award FA8750-12-2-0331, and gifts from Amazon Web Services, Google, IBM, SAP, The Thomas and Stacey Siebel Foundation, Adatao, Adobe, Apple, Inc., Blue Goji, Bosch, C3Energy, Cisco, Cray, Cloudera, EMC2, Ericsson, Facebook, Guavus, HP, Huawei, Informatica, Intel, Microsoft, NetApp, Pivotal, Samsung, Schlumberger, Splunk, Virdata and VMware.
