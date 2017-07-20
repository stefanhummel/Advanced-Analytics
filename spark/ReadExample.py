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
# SOURCE FILE NAME: ReadExample.py
# ----------------------------------------------------------------------------
 
from pyspark import SparkConf, SparkContext, SQLContext
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
# [default_schema].SPARK_TEST_DATA and invoke some simple Spark actions.
#
# (1) Initialize some Spark-specific components.
#
# (2) Create some random data points and write them into the
# table [default_schema].SPARK_TEST_DATA. If this table does not
# already exist, create it.
#
# (3) In order to read the data with the IBM IDAX data source, define
# the necessary property 'dbtable'. Declare also the correct data
# source, which is 'com.ibm.idax.spark.idaxsource'.
#
# (4) Count the data rows in the input table and store them in a
# variable. With this action, the data is read the first time.
#
# (5) Write the number of rows to the standard output, which is
# redirected to a file with a name of the form:
# [home directory]/spark/log/submission_[submission_id]/submission.out
#
# (6) Use the data frame a second time to select some specific columns.
#
# (7) Trigger a second action, which is to write the first
# 20 rows, ordered by the ID of each row, to standard output again.
#
# You can use the IBM dashDB Analytics API to invoke this application with this cURL command:
# curl -k -v -u "[userid]":"[password]" -X POST "https://[hostname]:[port]/dashdb-api/analytics/public/apps/submit"
# 	--header 'Content-Type:application/json;charset=UTF-8'
# 	--data '{
#         "appResource" : "ReadExample.py",
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
# ***** Change #2: Read data from a dashDB table instead of from a JSON file. *****
    df = sqlContext.read \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable=table) \
        .load()

    # (4) Trigger the action 'count' on the data frame.
    resultCount = df.count()

    # (5) Print the result of the count.
    print resultCount

    # (6) Select three columns and order the data by 'ID'.
    resultOrderBy = df.select("ID", "PIVOT1", "PIVOT2").orderBy("ID")

    # (7) Trigger the action 'show'.
    print resultOrderBy.show()
