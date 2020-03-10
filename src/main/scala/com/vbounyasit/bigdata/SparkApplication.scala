/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 8:22 PM
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

package com.vbounyasit.bigdata

import cats.implicits._
import com.vbounyasit.bigdata.ETL._
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.args.base.OutputArgumentsConf
import com.vbounyasit.bigdata.config.data.JobsConfig.{JobConf, JobSource}
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.config.{ConfigDefinition, ConfigsExtractor, ConfigurationsLoader, OutputTablesGenerator}
import com.vbounyasit.bigdata.exceptions.ErrorHandler
import com.vbounyasit.bigdata.exceptions.ErrorHandler._
import com.vbounyasit.bigdata.providers.{LoggerProvider, SparkSessionProvider}
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.utils.MonadUtils._
import com.vbounyasit.bigdata.utils.{CollectionsUtils, DateUtils}
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * A class representing a submitted Spark application.
  */
abstract class SparkApplication[U, V] extends SparkSessionProvider with ETL[U, V] with LoggerProvider {

  /**
    * The configuration files definition
    */
  val configDefinition: ConfigDefinition

  /**
    * The defined execution plans
    *
    * @param spark an implicit spark session
    * @return A JobName/ExecutionPlan Map
    */
  def executionPlans(implicit spark: SparkSession): Map[String, ExecutionConfig]

  /**
    * Loads a set of parameters needed for the ETL Operation
    *
    * through : config files loading, argument parsing, execution parameters creation, etc...
    *
    * @param args The list of arguments to parse
    * @return An ExecutionData object containing all the required parameters
    */
  protected def loadExecutionData(args: Array[String]): ExecutionData[_, _] = {

    def toEitherOfOptional(value: Option[Either[ErrorHandler, _]]): Either[ErrorHandler, Option[_]] =
      value match {
        case Some(either) => either.map(Some.apply)
        case None => Right(None)
      }

    (for {
      /**
        * Loading configuration files
        */
      loadedConfigurations <- ConfigurationsLoader(configDefinition)

      /**
        * Parsing global Application configuration
        */
      parsedApplicationConfiguration <- toEitherOfOptional(configDefinition.applicationConf)

      /**
        * Parsing global Application argument parameters
        */
      parsedApplicationArguments <- toEitherOfOptional {
        configDefinition.applicationArguments.map(
          _.argumentParser.parseArguments(
            loadedConfigurations.sparkParamsConf.appName,
            args
          ))
      }

      /**
        * Compulsory arguments parsing (like environment, job to launch, etc)
        */
      parsedBaseArgument <- {
        val argumentsConfiguration = new OutputArgumentsConf
        argumentsConfiguration.argumentParser.parseArguments(
          loadedConfigurations.sparkParamsConf.appName,
          args
        )
      }

      /**
        * Getting the list of jobs to compute
        */
      tablesToCompute <- {
        val parsedApplicationInfo = (parsedApplicationConfiguration, parsedApplicationArguments)
        /**
          * The output tables defined in configDefinition must take a conf or an argument object to be valid
          */
        val configuredOutputTables: OutputTables = configDefinition.outputTables match {
          case tableInfo@OutputTablesGenerator(Some(_), None) => parsedApplicationInfo match {
            case (Some(appConf), None) => tableInfo.applyFunction1(appConf)
            case (None, Some(arguments)) => tableInfo.applyFunction1(arguments)
            case (Some(appConf), Some(arguments)) => Try(tableInfo.applyFunction1(appConf)).getOrElse(tableInfo.applyFunction1(arguments))
            case _ => None
          }
          case tableInfo@OutputTablesGenerator(None, Some(_)) => parsedApplicationInfo match {
            case (Some(appConf), Some(arguments)) => tableInfo.applyFunction2(appConf, arguments)
            case _ => None
          }
        }
        /**
          * The output tables will be either the parameters given in the command line or the output tables defined in ConfigDefinition
          */
        val output: OutputTables = configuredOutputTables match {
          case None => (parsedBaseArgument.table, parsedBaseArgument.database) match {
            case ("N/A", _) | (_, "N/A") => None
            case (database, table) => Some(Seq(TableMetadata(database, table)))
          }
          case resultingTables => resultingTables
        }
        optionToEither(output, NoOutputTablesSpecified())
      }

      /**
        * Loading jobs conf
        */
      jobsConf <- ConfigsExtractor.getJobs(tablesToCompute.map(_.table), loadedConfigurations.jobsConf)

      /**
        * Loading execution parameters
        */
      executionsParameters <- {
        implicit val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)
        getMapSubList(tablesToCompute.map(_.table).toList, executionPlans, ExecutionPlanNotFoundError)
      }

      /**
        * Merging with the list of output tables
        */
      jobsConfWithOutputMetadata <- CollectionsUtils.mergeByKeyStrict(
        jobsConf,
        tablesToCompute.map(metadata => (metadata.table, metadata)).toMap,
        MergingMapKeyNotFound
      )

      /**
        * Merging with the execution parameters
        */
      withExecutionParameters <- CollectionsUtils.mergeByKeyStrict(
        jobsConfWithOutputMetadata,
        executionsParameters,
        MergingMapKeyNotFound
      )


      /**
        * Parsing the job arguments and configurations
        */
      jobFullExecutionParameters <- {

        withExecutionParameters
          .values
          .map {
            case ((jobConf, tableMetadata), executionConfig) =>
              val parsedJobConfiguration: Either[ErrorHandler, Option[_]] = toEitherOfOptional(executionConfig.additionalConfig)
              val parsedJobArguments: Either[ErrorHandler, Option[_]] = toEitherOfOptional(executionConfig.additionalArguments.map(argsConf => {
                argsConf.argumentParser.parseArguments(
                  loadedConfigurations.sparkParamsConf.appName,
                  args
                )
              }))
              for {
                jobConfig <- parsedJobConfiguration
                jobArguments <- parsedJobArguments
              } yield
                JobExecutionParameters(
                  jobConf,
                  tableMetadata,
                  JobParameters(jobConfig, jobArguments),
                  executionConfig.executionFunction
                )
          }.toList.sequence
      }
    } yield {
      ExecutionData(
        loadedConfigurations,
        parsedApplicationConfiguration,
        parsedApplicationArguments,
        jobFullExecutionParameters.toSeq,
        loadedConfigurations.spark,
        parsedBaseArgument.env
      )
    })
    match {
      case Right(executionData) => executionData
      case Left(errorHandler) => throw errorHandler
    }
  }

  /**
    * Extracts data from a provided sources configuration
    *
    * @param jobName        The Job name
    * @param jobSourcesConf The Job input sources configuration
    * @param sourcesConf    The different input sources configuration
    * @param env            The environment in which we want to extract the input sources from
    * @param spark          An implicit spark session
    * @return A Map of sourceName/SourcePipeline containing the extracted sources.
    */
  override def extract(jobName: String,
                       jobSourcesConf: List[JobSource],
                       sourcesConf: SourcesConf,
                       env: String)(implicit spark: SparkSession): Sources = {
    ConfigsExtractor.getSources(jobName, jobSourcesConf, sourcesConf, env)
      .info(s"Successfully extracted sources for job $jobName")
  }

  /**
    * Apply transformations to a given set of sources
    *
    * @param jobName          The Job name
    * @param sources          The extracted input sources
    * @param executionPlan    The execution plan to apply
    * @param exportDateColumn An optional date column name to tie the result computation date
    * @param spark            An implicit spark session
    * @return The resulting DataFrame
    */
  override def transform(jobName: String,
                         sources: Sources,
                         executionPlan: ExecutionPlan,
                         outputColumns: Option[Seq[String]],
                         exportDateColumn: Option[String])(implicit spark: SparkSession): DataFrame = {

    def getSource(sourceName: String): EitherRP = {
      optionToEither(sources.get(sourceName), JobSourcesNotFoundError(jobName, sourceName))
    }

    def selectOutputColumns: DataFrame => DataFrame = dataFrame => {
      outputColumns match {
        case None | Some(Nil) => dataFrame
        case Some(columns) => dataFrame.select(columns.map(col): _*)
      }
    }

    def attachExportDate: DataFrame => DataFrame = dataFrame => {
      exportDateColumn match {
        case Some(column) => dataFrame.withColumn(column, lit(DateUtils.today(datePattern.pattern)))
        case None => dataFrame
      }
    }

    selectOutputColumns(
      attachExportDate(
        executionPlan
          .getExecutionPlan(getSource)
          .info("Successfully loaded Execution plan for data transformation")
          .transform
      )
    )
  }
}
