#!/bin/bash

/root/mapreduce/bin/stop-mapred.sh
/root/mapreduce/bin/start-mapred.sh

/root/mapreduce/bin/hadoop distcp s3n://imagenet-train-all-scaled-tar/imagenet-train-all-scaled-tar /
/root/mapreduce/bin/hadoop distcp s3n://imagenet-validation-all-scaled-tar/imagenet-validation-all-scaled-tar /
