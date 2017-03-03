---
title: KeystoneML - Example Pipelines
layout: default
---

# KeystoneML Examples

KeystoneML ships with a number of example ML pipelines that reproduce recent academic results in several fields. 
Once you've built the project, you can run the following examples locally or on a Spark cluster.

## Image Classification

Keystone ships with a number of example pipelines for Image Classification. These pipelines are designed to scale well across a cluster of commodity machines.

### Dataset: MNIST

The MNIST dataset contains 50,000 training images of hand-written digits along with manually created labels.
As of 2015, this benchmark is considered small by image classification standards and we can train a good classification pipeline on it on a laptop in under a minute.

{% highlight bash %}
examples/images/mnist_random_fft.sh
{% endhighlight %}

This example runs the [MnistRandomFFT](https://github.com/amplab/keystone/blob/master/src/main/scala/keystoneml/pipelines/images/mnist/MnistRandomFFT.scala) pipeline which uses random feature maps and a linear model to build an optical character recognition pipeline for handwritten digits.

This example should achieve about 4% test error in less than a minute, and increasing the block size to 4096 and increasing the number of FFTs to 20 or more generally makes it perform even better.


### Dataset: CIFAR-10
KeystoneML ships with several pipelines for the CIFAR-10 dataset. The most advanced pipeline can be trained with the following:

{% highlight bash %}
examples/images/cifar_random_patch.sh
{% endhighlight %}

This example runs the [RandomPatchCifar](https://github.com/amplab/keystone/blob/master/src/main/scala/keystoneml/pipelines/images/cifar/RandomPatchCifar.scala) pipeline builds a 10-class image classification pipeline over a training set of 50,000 tiny images. It uses convolution filters that have been randomly sampled from the training data as feature extractors.

This will take a bit longer to run and you may want to launch a small (8-node) cluster to finish it. It should achieve close to state-of-the-art performance of 15% error.

### Dataset: VOC 2007

{% highlight bash %}
examples/images/voc_sift_fisher.sh
{% endhighlight %}

This example runs the [VOCSIFTFisher](https://github.com/amplab/keystone/blob/master/src/main/scala/keystoneml/pipelines/images/voc/VOCSIFTFisher.scala) pipeline which builds a multi-class image classification pipeline over a training set of 5,000 full sized images. It uses SIFT Feature extraction and Fisher Vectors to featurize the training data.

You should see an [MAP](http://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision) around 58% for this 20 class classification problem, and the pipeline will run in about 15 minutes on a cluster of 16 `cc2.8xlarge` machines on Amazon EC2. 

## Text


### Dataset: 20 Newsgroups
{% highlight bash %}
examples/text/newsgroups_ngrams_tfidf.sh
{% endhighlight %}

This example runs the [NewsgroupsPipeline](https://github.com/amplab/keystone/blob/master/src/main/scala/keystoneml/pipelines/text/NewsgroupsPipeline.scala) pipeline which builds a 20 class text classification pipeline based on n-gram features and TF-IDF smoothing on a dataset of thousands of newsgroup posts. 


