/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 8:19 PM
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

import com.vbounyasit.bigdata.ETL.{ExecutionParameters, OptionalJobParameters}
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.config.data.JobsConfig.JobConf
import com.vbounyasit.bigdata.config.{ConfigsExtractor, ConfigurationsLoader}
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.ReadDataFramesFromFilesError
import com.vbounyasit.bigdata.testing.common.{HiveEnvironment, TestComponents}
import com.vbounyasit.bigdata.testing.data.JobTableMetadata
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameWriter
import com.vbounyasit.bigdata.transform.TransformOps._
import com.vbounyasit.bigdata.{Sources, SparkApplication}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * A trait representing Integration tests for a given application
  */
trait JobsTestGenerator extends TestComponents {

  /**
    * The application to generate tests for.
    */
  val sparkApplication: SparkApplication[_, _]

  /**
    * The hive environment object used.
    */
  val hiveEnvironment: HiveEnvironment

  /**
    * The DataFrameWriter object used.
    */
  val dataFrameWriter: DataFrameWriter

  /**
    * An optional custom argument object we want to use in our tests.
    */
  val defaultCustomArgument: Option[_] = None

  /**
    * An optional application conf file we want to use in our tests.
    */
  val defaultApplicationConf: Option[_] = None

  /**
    * Executing the tests.
    */
  override final def executeTests(env: String = environment): Unit = {
    val loadedConfigurations = ConfigurationsLoader(sparkApplication.configDefinition, useLocalSparkParams = true)
      .info("Successfully loaded configurations for local run.")

    implicit val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)

    "Hive Environment" should "successfully setup all the required source tables" in {
      hiveEnvironment.setupEnvironment(loadedConfigurations.jobsConf, loadedConfigurations.sourcesConf, env)
      logger.info("Hive environment successfully setup")
    }

    val optionalParameters: OptionalJobParameters[Any, Any] = OptionalJobParameters(defaultApplicationConf, defaultCustomArgument)

    sparkApplication.executionPlans.foreach {
      case (jobName, ExecutionParameters(executionFunction, _)) => {
        s"${jobName.capitalize}" should "Compute the right Result" in {
          /**
            * Extracting the job configuration
            */
          val extractedJobConf = ConfigsExtractor
            .getJob(jobName, loadedConfigurations.jobsConf)
          val jobConf: JobConf = extractedJobConf.copy(sources =
            extractedJobConf.sources.map(source => source.copy(
              sourceName = s"${jobName}_${source.sourceName}")
            )
          )

          /**
            * Loading our Expected output DataFrame from resources file
            */
          val outputJobTableMetadata: JobTableMetadata = JobTableMetadata(jobName, "job_results", jobName)
          val loadedOutputDataFrame: Either[ReadDataFramesFromFilesError, DataFrame] =
            hiveEnvironment.dataFrameLoader.loadDataFrames(Seq(outputJobTableMetadata), "out") match {
              case List(eitherDf) => eitherDf.right.map(_._2)
              case _ => Left(hiveEnvironment.dataFrameLoader.getFileNotFoundException(outputJobTableMetadata))
            }
          /**
            * Given
            */
          //extract
          //todo figure out why the row order is changed after extraction
          val sources: Sources = sparkApplication.extract(
            jobName,
            jobConf.sources,
            {
              //adding in the job identifier, to be able to query the right table
              val sourcesConf = loadedConfigurations.sourcesConf
              sourcesConf.copy(tables =
                sourcesConf.tables.map {
                  case (sourceName, tableInfo) =>
                    (s"${jobName}_$sourceName", tableInfo.copy(table = s"${jobName}_${tableInfo.table}"))
                }
              )
            },
            env)
            //removing that job name identifier from the source names, since we don't want it there
            .map {
              case (sourceName, pipeline) => (sourceName.replace(s"${jobName}_", ""), pipeline)
            }

          /**
            * Checking the resources files for unfilled source column values
            */
          val unfilledFields = sources.filter {
            case (_, pipeline) =>
              val df = pipeline.transform
              val filterPredicate = df.schema.map(_.name).foldLeft(lit(false))(
                (acc, column) => acc or col(column) === defaultColumnValue
              )
              df.filter(filterPredicate).collect.nonEmpty
          }.keys

          if (unfilledFields.nonEmpty)
            fail(s"Could not finish the test, unfilled fields found for sources : ${unfilledFields.mkString(",")}.")

          /**
            * When
            */
          //transform
          val resultDataFrame = sparkApplication.transform(
            jobName,
            sources,
            executionFunction(optionalParameters),
            Some(jobConf.outputMetadata.outputColumns),
            None
          )

          /**
            * Then
            */
          val jobOutputId = jobConf.outputMetadata.testIdColumn

          /**
            * If we have found an output file in our Resources, then
            * we load it as our "expected" DataFrame, and compare it with our
            * result, otherwise we create a new output file from this result.
            */
          loadedOutputDataFrame match {
            case Right(dataFrame) =>
              //schema testing
              dataFrame.schema.map(_.name) should contain theSameElementsAs resultDataFrame.schema.map(_.name)
              //content testing
              val expectedDataFrame = dataFrame.select(resultDataFrame.schema.map(_.name).map(col): _*)
              val expectationDiff = expectedDataFrame.except(resultDataFrame)
              val resultDiff = resultDataFrame.except(expectedDataFrame)

              /**
                * If an output index column is specified, it will be used to tell
                * where exactly the expected/obtained differences are, otherwise,
                * we use the row number
                */
              if (resultDiff.count() > 0) {
                logger.info("DataFrame comparison found some differences. showing results...")
                if (jobOutputId.nonEmpty) {
                  showComparisonDifferences(expectationDiff, resultDiff, jobOutputId)
                } else {
                  val outputId = "row_number"

                  def computeRowNum: DataFrame => DataFrame = dataFrame => {
                    val uidCol = "uid"
                    dataFrame
                      .withColumn(uidCol, monotonically_increasing_id())
                      .withColumn(outputId, row_number().over(Window.orderBy(uidCol)))
                      .drop(uidCol)
                  }

                  showComparisonDifferences(
                    (expectationDiff ==> computeRowNum).transform,
                    (resultDiff ==> computeRowNum).transform,
                    outputId
                  )
                }
                fail("Resulting DataFrame was not equal to Expected DataFrame.")
              } else {
                logger.info("Spark Integration test passed.")
                succeed
              }
            case Left(_) =>
              val columnsToSelect: Seq[String] = {
                if (jobOutputId.nonEmpty)
                  jobOutputId +: resultDataFrame.schema.map(_.name).filter(_ != jobOutputId)
                else
                  resultDataFrame.schema.map(_.name)
              }
              dataFrameWriter.writeDataFrameToResources(
                jobName,
                outputJobTableMetadata.database,
                outputJobTableMetadata.table,
                "out",
                resultDataFrame.select(columnsToSelect.map(col): _*)
              )
              fail("No output file could be read for this Job. Creating a file from the result obtained in this test.")
          }
        }
      }
    }
  }

  /**
    * A function that explodes each row into rows
    * in a row-per-column fashion with [index - column_name - column_value) format
    *
    * @param idColumn        the name of the index column
    * @param columnValueName the name that will be given to the column values
    * @return A DataFrame => DataFrame transformer function
    */
  private def getColumnStats(idColumn: String, columnValueName: String): DataFrame => DataFrame = dataFrame => {
    dataFrame
      .select(col(idColumn),
        explode(
          array(dataFrame.schema.map(column =>
            array(lit(column.name), col(column.name))): _*)
        ).as("column_stats")
      )
      .withColumn("column_name", col("column_stats")(0))
      .withColumn(columnValueName, col("column_stats")(1))
      .drop("column_stats")
      .filter(col("column_name") =!= idColumn)
  }

  /**
    * Shows a comparison between two DataFrames, featuring [column_name - expectation - reality]
    * for each differences
    *
    * @param expectation The expected DataFrame
    * @param reality     The real DataFrame obtained
    * @param indexColumn The name of our unique column
    */
  private def showComparisonDifferences(expectation: DataFrame, reality: DataFrame, indexColumn: String): Unit = {
    val expectationValues = (expectation ==> getColumnStats(indexColumn, "expected")).transform
    val realityValues = (reality ==> getColumnStats(indexColumn, "obtained")).transform

    expectationValues
      .join(realityValues, Seq(indexColumn, "column_name"), "outer")
      .filter(col("expected") =!= col("obtained"))
      .orderBy(indexColumn, "column_name")
      .show
  }
}
