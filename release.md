---
title: KeystoneML - Large scale end-to-end machine pipelines.
layout: default
---

# Release Notes

## Version 0.2 - 2015-09-18

* Some pipeline APIs have been updated and moved to a `workflow` module. In general, pipeline construction is more consistent and uses `andThen` regardless of what you are chaining to a pipeline.
* `Pipeline.gather` operator added to simplify
* Examples updated to reflect these API changes.
* Bug fixes in example pipelines.
* A fix was added to make weighted least squares solver work on problems with one block.
* Fixes to build process.
* Amazon Reviews example classification pipeline added.
* Contributors include: Tomer Kaftan, Evan Sparks, and Shivaram Venkataraman.