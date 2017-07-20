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
# SOURCE FILE NAME: ReadExampleJdbc.py
# ----------------------------------------------------------------------------
 
from pyspark import SparkConf, SparkContext, SQLContext
#from com.ibm.idax.spark.examples.utilities.Helper import initializationDashDB

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
# [DEFAULT SCHEMA].SPARK_TEST_DATA and invokes some simple Spark actions.
#
# (1) First of all some Spark specific components will be initialized.
#
# (2) After that we will create some random data points and write them
# into the table [DEFAULT_SCHEMA].SPARK_TEST_DATA if it doesn't
# exist already.
#
# (3) In order to read the data with the IBM Idax data source, we define
# the necessary properties like 'dbtable' and 'mode' (in this
# case we read the data coLocated via JDBC). We also have to
# declare the correct datasource => 'com.ibm.idax.spark.idaxsource'.
#
# (4) Some columns that we like to use for our actions will be
# selected and ordered by its 'ID'.
#
# (5) The data rows from the table will be counted. With this
# action the data is actually read the first time from the table.
# The result of the counted rows will be printed to the standard output.
# In this case this means that the count will be written into a file which
# can be found in the home directory of the user under:
# [home directory]/spark/log/...
#
# (6) A second action will be triggered. In this case we will print the first
# 20 rows to standard output. This means that they will be written into a file
# which can be found in the home directory of the user under:
# [home directory]/spark/log/...
#
# A sample invocation via the CLUES REST API can be done like this:
# curl -k -v -u cluesuser:abcd1234 -X POST https://localhost:8443/clues/public/jobs/submit
#     --header "Content-Type:application/json;charset=UTF-8"
#     --data '{
#         "appResource" : "ReadExampleJdbc.py",
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
    #table = initializationDashDB(sc, sqlContext)
    table = "SPARK_TEST_DATA"
    
    # (3) Define parameters to read the data from the DB with idaxsource.
    df = sqlContext.read \
        .format("com.ibm.idax.spark.idaxsource") \
        .options(dbtable=table, mode="JDBC") \
        .load()
            
    # (4) Select the columns from the table that have to be read.
    results = df.select("ID", "PIVOT1", "PIVOT2").orderBy("ID")
        
    # (5) Trigger the action 'count'.
    print results.count()
        
    #(6) Trigger the action 'show'.
    print results.show()
    
