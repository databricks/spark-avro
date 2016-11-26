#!/usr/bin/env bash

set -e

sbt \
    -Dhadoop.testVersion=$TEST_HADOOP_VERSION \
    -Dspark.testVersion=$TEST_SPARK_VERSION \
    -Davro.testVersion=$TEST_AVRO_VERSION \
    -DavroMapred.testVersion=$TEST_AVRO_MAPRED_VERSION \
    ++$TRAVIS_SCALA_VERSION coverage test coverageReport

sbt ++$TRAVIS_SCALA_VERSION scalastyle
