#!/bin/bash

CURRDIR=$(readlink -e `dirname %0`)

cd $CURRDIR/../../

sbt assembly

