# README

## Build status

[![Build Status](https://api.travis-ci.org/mauriciojost/spark-input-splitter.svg)](https://travis-ci.org/mauriciojost/spark-input-splitter)

## Description

This is a non-splittable-files splitter project for Spark. 


```
         INPUT

        +-+ +-+
        | | | |
        +-+ +-+
        +-+ +-+
smalls  | | | |
        +-+ +-+      +------------------------+
        +-+ +-+      |                        |
        | | | |  +--------------------------------->
        +-+ +-+      |                        |
                     |  spark-input-splitter  |         RDD
        +-+ +-+      |                        |
        | | | |  +--------------+   +-------------->
        | | | |      |          |   |         |
        | | | |      +----------|---|---------+
  bigs  | | | |                 |   |
        | | | |                 v   +
        | | | |
        | | | |                +-+ +-+
        +-+ | |                | | | |
            | |                +-+ +-+
            | |                | | | |
            | |                +-+ +-+  SPLITS
            +-+                | | | |
                               +-+ +-+
                               | | | |
                               +-+ +-+
                                   | |
                                   +-+
                                   | |
                                   +-+

```

It comes with a `split writer` and a `split reader` to be used in such order. 

1. First `splits` must be written with `split writer` from `bigs` (big non-splittable files identified using a `condition`). 

2. Then `split reader` is used to read `splits` together with `smalls`, resulting in an RDD equivalent to one generated from `input`.

## Use cases

You will find useful this library on the following use cases:

- Your Spark jobs use many non-splittable input files whose sizes are not homogeneous, causing your Spark phase to be sometimes delayed for the late processing of some big file.


