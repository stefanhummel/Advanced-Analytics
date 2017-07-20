#!/usr/bin/python
# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# © Copyright IBM Corp. 2016 All rights reserved.
#
# The following sample of source code ("Sample") is owned by International
# Business Machines Corporation or one of its subsidiaries ("IBM") and is
# copyrighted and licensed, not sold. You may use, copy, modify, and
# distribute the Sample in any form without payment to IBM, for the purpose of
# assisting you in the development of your applications.
#
# The Sample code is provided to you on an "AS IS" basis, without warranty of
# any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR
# IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do
# not allow for the exclusion or limitation of implied warranties, so the above
# limitations or exclusions may not apply to you. IBM shall not be liable for
# any damages you suffer as a result of using, copying, modifying or
# distributing the Sample, even if IBM has been advised of the possibility of
# such damages.
#
# SOURCE FILE NAME: ReadWriteExampleKMeansJdbc.py
# ----------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline

from com.ibm.idax.spark.examples.utilities.helper import initializationDashDB

# ----------------------------------------------------------------------------
# Following example demonstrates how the IBM Idax data source for
# JDBC coLocation & fast data reading can be used in order to
# read some data from a table in DashDB.
#
#
# Attention: The first time running one of the provided examples
# will create a table with some data for the examples. This may
# take several minutes.
#
#
# (0) This example is reading some randomly created data from the table
# [DEFAULT_SCHEMA].SPARK_TEST_DATA, uses the data to create a K-Means
# model and does some predictions with the just created model on the
# same data points. Each point and its predicted cluster centroid will
# be stored in a table [DEFAULT_SCHEMA].SPARK_TEST_DATA_PREDICT.
#
# (1) First of all some Spark specific components will be initialized.
#
# (2) After that we will create some random data points and write them
# into the table [DEFAULT SCHEMA].SPARK_TEST_DATA if it doesn't
# exist already. The random data points (x:=x-axis, y:=y-axis, r:=radius)
# will be created around following centroids with different radii:
# (x=0, y=0, r=2), (x=0, y=10, r=3), (x=10, y=10, r=1), (x=10, y=0, r=2)
# and (x=5, y=5, r=1). The random points are created with help of
# 'scala.util.Random.nextDouble()'. This means the points around the
# centroids are uniform distributed. The used K-Means algorithm for this
# application should calculate similar centroids again.
#
# (3) In order to read the data with the IBM Idax data source, we define
# the necessary properties like 'dbtable' and 'mode' (in this
# case we read the data coLocated via JDBC). We also have to
# declare the correct datasource => 'com.ibm.idax.spark.idaxsource'.
#
# (4) Since we use a Spark pipeline in step (6) we build one of the
# components for it. First, we create a VectorAssembler that will
# take the input columns 'PIVOT1' and 'PIVOT2' from the table and build
# a vector representation of it.
#
# (5) Now we define the parameters for our Spark K-Means clustering
# algorithm, which will be the second component in the pipeline.
#
# (6) Combine all components within a single pipeline.
#
# (7) All components in the pipeline are ready to go. We initialize
# the building of the model by providing the necessary input data.
# Now, the data will be actually read from the database.
#
# (8) We are curious what are the cluster centroids of our data points.
# They will be printed out to standard output. In this case this means that
# they will be written into a file which can be found in the home directory
# of the user under:
# [home directory]/spark/log/...
#
# (9) The just trained model is now taken for some test predictions
# on the same data that was used for training. The results will be
# stored in table [DEFAULT SCHEMA].SPARK_TEST_DATA_PREDICT. The
# necessary parameters for the IBM Idax data source are defined.
#
# A sample invocation via the CLUES REST API can be done like this:
# curl -k -v -u cluesuser:abcd1234 -X POST https://localhost:8443/clues/public/jobs/submit
#     --header "Content-Type:application/json;charset=UTF-8"
#     --data '{
#         "appResource" : "ReadWriteExampleKMeansJdbc.py",
#         "sparkProperties" : {
#             "sparkSubmitPyFiles" : "example_utilities.egg"
#         }
#     }'
# ----------------------------------------------------------------------------

# (0) Starting point for our Spark application.
if __name__ == '__main__':
    
    # (1) Initialize Spark specific components.
    sparkConf = SparkConf()
    sc = SparkContext(conf = sparkConf)
    sqlContext = SQLContext(sc)
    
    # (2) Create random data points and store them in a DashDB table.
    table = initializationDashDB(sc, sqlContext)
    
    # (3) Define parameters to read the data from the DB with idaxsource.
    print "Training"
    inputData = sqlContext.read \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable = "SPARK_TEST_DATA", mode = "JDBC") \
        .load()
            
    # (4) Create a Spark VectorAssembler with the input columns and
    # define its output column.
    assembler = VectorAssembler(
            inputCols = ["PIVOT1", "PIVOT2"],
            outputCol = "features")

    # (5) Define parameters for our K-Means algorithm.
    clustering = KMeans(
            featuresCol = "features",
            k = 5,
            maxIter = 3)

    # (6) Build the pipeline with all steps.
    pipe = Pipeline(stages = [assembler, clustering])

    # (7) Build the K-Means model.
    model = pipe.fit(inputData)

    # (8) Print the cluster centroids.
    print model.stages[1].clusterCenters()

    # (9) Test the algorithms by doing some predictions.
    print "Predicting"
    output = model.transform(inputData).select("ID", "prediction")
    output.write \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable = table + "_PREDICT") \
        .mode("overwrite") \
        .save()

    print "Results are stored in table " + table + "_PREDICT"
    