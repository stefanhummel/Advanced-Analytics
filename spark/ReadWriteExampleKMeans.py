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
# SOURCE FILE NAME: ReadWriteExampleKMeans.py
# ----------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline

from com.ibm.idax.spark.examples.utilities.helper import initializationDashDB

# ----------------------------------------------------------------------------
# This example demonstrates how the IBM in-database analytic
# functions can be used to read data from a table in dashDB.
#
#
# Attention: The first time this application is run it will create
# a table with some data. This might take several minutes.
#
# The steps carried out by this application are:
#
# (0) Read randomly created data from the table
# [default_schema].SPARK_TEST_DATA, use the data to create a k-means
# model, and make some predictions based on the created model. Each
# point and its predicted cluster centroid are stored in the table
# [default_schema].SPARK_TEST_DATA_PREDICT.
#
# (1) Initialize the Spark-specific components.
#
# (2) Create some random data points and write them into the table
# [default_schema].SPARK_TEST_DATA. If this table does not already exist,
# create it. The random data points are created around following
# centroids (x,y) with different radii (r):
#   (x=0,  y=0,  r=2)
#   (x=0,  y=10, r=3)
#   (x=10, y=10, r=1)
#   (x=10, y=0,  r=2)
#   (x=5,  y=5,  r=1)
# The random points are created with help of 'scala.util.Random.nextDouble()'.
# This means the points around each centroid are uniformly distributed.
# The k-means algorithm used by this application should calculate
# similar centroids again.
#
# (3) In order to read the data with the IBM IDAX data source, define
# the necessary property 'dbtable'. Declare also the correct data
# source, which is 'com.ibm.idax.spark.idaxsource'.
#
# (4) Because this example uses a Spark pipeline in step 6, build
# one of the components that the pipeline will need. First, create a
# VectorAssembler that takes the input columns 'PIVOT1' and 'PIVOT2'
# from the table and builds a vector representation of the two columns.
#
# (5) Define the parameters for the k-means clustering algorithm,
# which is the second component in the pipeline.
#
# (6) Combine all the components within a single pipeline.
#
# (7) All components in the pipeline are ready to go. Initialize the
# building of the model by providing the necessary input data. Now,
# the data is read from the database.
#
# (8) The cluster centroids of the data points are written out to
# standard output, which is a file with a name of the form:
# [home directory]/spark/log/submission_[submission_id]/submission.out
#
# (9) The example uses the model, which has just been trained, to
# make some test predictions based on the same data that was used for
# the training. The results are stored in table
# [default_schema].SPARK_TEST_DATA_PREDICT.
#
# You can use the IBM dashDB Analytics API to invoke this application with this cURL command:
# curl -k -v -u "[userid]":"[password]" -X POST "https://[hostname]:[port]/dashdb-api/analytics/public/apps/submit"
# 	--header 'Content-Type:application/json;charset=UTF-8'
# 	--data '{
#         "appResource" : "ReadWriteExampleKMeans.py",
#         "sparkProperties" : {
#             "sparkSubmitPyFiles" : "example_utilities.egg"
#         }
#     }'
# ----------------------------------------------------------------------------

# (0) Starting point for our Spark application.
if __name__ == '__main__':
    
    # (1) Initialize Spark specific components.
# ***** Change #1: Remove the master and application name. *****
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)
    
    # (2) Create random data points and store them in a DashDB table.
    table = initializationDashDB(sc, sqlContext)
    
    # (3) Define parameters to read the data from the DB with idaxsource.
    print "Training"
# ***** Change #2: Read data from a dashDB table instead of from a JSON file. *****
    inputData = sqlContext.read \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable="SPARK_TEST_DATA") \
        .load()
            
    # (4) Create a Spark VectorAssembler with the input columns and
    # define its output column.
    assembler = VectorAssembler(
            inputCols=["PIVOT1", "PIVOT2"],
            outputCol="features")

    # (5) Define parameters for our K-Means algorithm.
    clustering = KMeans(
            featuresCol="features",
            k=5,
            maxIter=3)

    # (6) Build the pipeline with all steps.
    pipe = Pipeline(stages=[assembler, clustering])

    # (7) Build the K-Means model.
    model = pipe.fit(inputData)

    # (8) Print the cluster centroids.
    print model.stages[1].clusterCenters()

    # (9) Test the algorithms by doing some predictions.
    print "Predicting"
    output = model.transform(inputData).select("ID", "prediction")
# ***** Change #3: Write data to a dashDB table instead of to a JSON file. *****
    output.write \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable=table + "_PREDICT") \
        .mode("overwrite") \
        .save()

    print "Results are stored in table " + table + "_PREDICT"
    