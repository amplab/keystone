---
title: KeystoneML - Example Pipelines
layout: default
---

# KeystoneML Examples

KeystoneML ships with a number of example ML pipelines that reproduce recent academic results in several fields. 

## Image Classification

### Dataset: MNIST

The MNIST dataset contains 50,000 training images of hand-written digits along with manually created labels.
As of 2015, this benchmark is considered small by image classification standards and we can train a good classification pipeline on it on a laptop in under a minute.

{% highlight bash %}
# Get the data from S3
wget http://mnist-data.s3.amazonaws.com/train-mnist-dense-with-labels.data
wget http://mnist-data.s3.amazonaws.com/test-mnist-dense-with-labels.data

KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
{% endhighlight %}

This example should achieve about 4% test error in less than a minute, and increasing the block size to 4096 and increasing the number of FFTs to 20 or more generally makes it perform even better.


### Dataset: CIFAR-10
KeystoneML ships with several pipelines for the CIFAR-10 dataset. The most advanced pipeline can be trained with the following:

{% highlight bash %}
#Get the data
wget http://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz

#Decompress it
tar zxvf cifar-10-binary.tar.gz
cat cifar-10-batches-bin/data_batch*.bin > ./cifar_train.bin
mv cifar-10-batches-bin/test_batch.bin ./cifar_test.bin

#Run the pipeline
KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.cifar.RandomPatchCifar \
  --trainLocation ./cifar_train.bin \
  --testLocation ./cifar_test.bin \
  --numFilters 10000
  --lambda 3000
{% endhighlight %}

This will take a bit longer to run and you may want to launch a small (8-node cluster) to finish it. It should achieve close to state-of-the-art performance of 15% error.

### Dataset: VOC 2007

{% highlight bash %}
#Get the data
wget https://s3-us-west-2.amazonaws.com/voc-data/VOCtrainval_06-Nov-2007.tar
wget https://s3-us-west-2.amazonaws.com/voc-data/VOCtest_06-Nov-2007.tar

#Copy to HDFS
/root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal VOCtrainval_06-Nov-2007.tar /data/
/root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal VOCtest_06-Nov-2007.tar /data/


#Run the pipeline
KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.voc.VOCSIFTFisher \
  --trainLocation /data/VOCtrainval_06-Nov-2007.tar \
  --testLocation /data/VOCtest_06-Nov-2007.tar \
  --labelPath src/test/resources/images/voclabels.csv \
  --numParts 200
{% endhighlight %}

You should see an [MAP](http://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision) around 58% for this 20 class classification problem, and the pipeline will run in about 15 minutes on a cluster of 16 `cc2.8xlarge` machines on Amazon EC2. 


### Dataset: ImageNet ILSVRC2012

## Text

### Dataset: 20 Newsgroups


## Speech

### Dataset: TIMIT
