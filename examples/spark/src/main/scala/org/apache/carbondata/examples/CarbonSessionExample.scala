/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark/src/main/resources/log4j.properties")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    val spark = ExampleUtils.createSparkSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("error")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession, formatSyntax: String = "stored as carbondata"): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    spark.sql("DROP TABLE IF EXISTS source")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE source(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | $formatSyntax
       """.stripMargin)

    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE source
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT charField, shortField, stringField, intField
         | FROM source
         | WHERE intfield=10
      """.stripMargin).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS source")
  }
}
