/*
 * Developed by Vibert Bounyasit
 * Last modified 24/02/19 21:55
 *
 * Copyright (c) 2019-present. All right reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vbounyasit.bigdata.testing

import com.typesafe.config.ConfigFactory
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.config.ConfigurationsLoader
import com.vbounyasit.bigdata.config.data.SparkParamsConf
import com.vbounyasit.bigdata.testing.common.TestComponents
import com.vbounyasit.bigdata.transform.pipeline.impl.SourcePipeline
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.generic.auto._

/**
  * A trait representing a set of unit tests we want to run.
  */
trait UnitTestGenerator extends TestComponents {

  /**
    * The Unit test definitions.
    *
    * @param spark The implicit spark session
    * @return A sequence of Unit test definitions to run.
    */
  def unitTestDefinitions(implicit spark: SparkSession): Seq[UnitTestDefinition]

  /**
    * The test execution
    */
  override final def executeTests(): Unit = {
    val loadedSparkConf: SparkParamsConf = ConfigurationsLoader.loadConfig[SparkParamsConf](
      "Spark local conf",
      ConfigFactory.load("spark_params_local")
    )
    implicit val spark: SparkSession = getSparkSession(loadedSparkConf)

    unitTestDefinitions
      .flatMap(_.unitTests)
      .foreach(unitTest => {
        unitTest.name should "Compute the right DataFrame" in {
          /**
            * Given
            */
          val inputDataFrame: DataFrame = unitTest.inputDf
          val outputDataFrame: DataFrame = unitTest.outputDf

          /**
            * When
            */
          val resultDataFrame: DataFrame = SourcePipeline(
            inputDataFrame,
            unitTest.transformer.pipeline
          ).transform

          /**
            * Then
            */
          //schema checking
          resultDataFrame.schema.map(_.name) should contain theSameElementsAs outputDataFrame.schema.map(_.name)
          //content checking
          val expectedDataFrame = outputDataFrame.select(resultDataFrame.schema.map(_.name).map(col): _*)
          val expectationDiff = expectedDataFrame.except(resultDataFrame)
          val resultDiff = resultDataFrame.except(expectedDataFrame)
          if ((expectationDiff union resultDiff).count() > 0) {
            logger.info("Expected DataFrame")
            expectedDataFrame.show()
            logger.info("Resulting DataFrame")
            resultDataFrame.show()
            fail("Expected dataFrame was not equal to Obtained dataFrame")
          } else {
            logger.info("Unit test has passed.")
          }
        }
      })
  }
}
