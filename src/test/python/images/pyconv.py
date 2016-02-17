#!/usr/bin/env python

import sys
import csv
import numpy as np
from scipy.misc import imread, imsave
from scipy.signal import convolve

#This script convolves an image in python and its output is used in
#the Convolver unit tests to ensure that convolver output matches
#an equivalent python call.

#This script was run from src/test/resources/images/ as:
#python pyconv.py gantrycrane.png convolved.gantrycrane.png convolved.gantrycrane.csv


def main():
  x = imread(sys.argv[1])
  k1 = np.array([i for i in range(27)]).reshape((3,3,3))
  out = np.sum(convolve(x, k1, mode='valid'), 2)
  imsave(sys.argv[2], out)
  cwriter = csv.writer(open(sys.argv[3], 'w'))
  for x in range(out.shape[0]):
    for y in range(out.shape[1]):
      cwriter.writerow([x,y,out[x,y]])
      

if __name__ == "__main__":
  main()
