---
title: KeystoneML Programming Guide
layout: default
---

# KeystoneML Programming Guide
This document covers key API concepts present in KeystoneML, and presents an overview of its components.

KeystoneML is a software framework designed to make building and deploying large scale machine learning pipelines easier.
To assist developers in this task we have created an API that simplifies common tasks and presents a unified interface
for all stages of the pipeline.

## Key API Concepts

### Transformers

A transformer is a function from A => B.

### Estimators

An estimator takes in training data, and outputs a Transformer. 

### Chaining and Building Pipelines

Pipelines are created by chaining transformers and estimators with the magic `then` methods.

## Included in KeystoneML

### Data Loaders
A data loader is the entry point for your data into a batch training pipeline.
We've included several data loaders for datasets that correspond to the example pipelines we've included.

### Nodes
A node is either a Transformer or an Estimator.
One of the main features of KeystoneML is the large library of nodes built for tasks like image processing, NLP, and speech. 

### Pipelines
We've included several example pipelines.

### Utilities
There are also some utilities for pipeline persistence.
