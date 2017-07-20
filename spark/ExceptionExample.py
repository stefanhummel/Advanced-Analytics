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
# SOURCE FILE NAME: ExceptionExample.py
# ----------------------------------------------------------------------------
 
from pyspark import SparkConf, SparkContext, SQLContext

# ----------------------------------------------------------------------------
# This example demonstrates how an application can throw an exception
# so that it is caught and recorded.
#
# The steps carried out by this application are:
#
# (0) This example demonstrates how an exception can be thrown.
#
# (1) It initializes some Spark-specific components.
#
# (2) It throws a runtime exception. This exception is caught and written to
# the info file, which has a name of the form:
# [home directory]/spark/log/submission_[submission_id]/submission.info
#
# You can use the IBM dashDB Analytics API to invoke this application with this cURL command:
# curl -k -v -u "[userid]":"[password]" -X POST "https://[hostname]:[port]/dashdb-api/analytics/public/apps/submit"
# 	--header 'Content-Type:application/json;charset=UTF-8'
# 	--data '{
#         "appResource" : "ExceptionExample.py",
#         "sparkProperties" : {
#             "sparkSubmitPyFiles" : "example_utilities.egg"
#         }
#     }'
# ----------------------------------------------------------------------------

# (0) Starting point for our Spark application.
if __name__ == '__main__':
    
    # (1) Initialize Spark specific components.
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    raise Exception('This is my thrown exception which will be caught by the IBM dashDB Analytics API!')
