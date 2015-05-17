# keystone
The biggest, baddest pipelines around.


# Example pipeline

### Build the Keystone project

```
./sbt/sbt assembly
```

### MNIST pipeline


```
# Get the data from S3
s3cmd get s3://mnist-data/train-mnist-dense-with-labels.data
s3cmd get s3://mnist-data/test-mnist-dense-with-labels.data

KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
```

## Running with spark-submit

You need to export `SPARK_HOME` to run Keystone using spark-submit. Having done
that you can similarly use run-pipeline.sh to launch your pipeline.

```
export SPARK_HOME=~/spark-1.3.1-bin-cdh4 # should match the version keystone is built with
KEYSTONE_MEM=4g ./bin/run-pipeline.sh \
  pipelines.images.mnist.MnistRandomFFT \
  --trainLocation ./train-mnist-dense-with-labels.data \
  --testLocation ./test-mnist-dense-with-labels.data \
  --numFFTs 4 \
  --blockSize 2048
```
