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

package com.vbounyasit.bigdata

import cats.implicits._
import com.vbounyasit.bigdata.ETL.{ExecutionData, ExecutionParameters, OptionalJobParameters}
import com.vbounyasit.bigdata.SparkApplication.ArgumentData
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.args.base.BaseArgumentParser
import com.vbounyasit.bigdata.args.{ArgumentsDefinition, DefaultArgument, ParserSetup}
import com.vbounyasit.bigdata.config.ConfigurationsLoader.loadConfig
import com.vbounyasit.bigdata.config.data.JobsConfig.JobSource
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.config.{ConfigDefinition, ConfigsExtractor, ConfigurationsLoader}
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.{ExecutionPlanNotFoundError, JobSourcesNotFoundError, ParseArgumentsError}
import com.vbounyasit.bigdata.providers.{LoggerProvider, SparkSessionProvider}
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.utils.{DateUtils, MonadUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures
import scopt.OParser

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
  def executionPlans(implicit spark: SparkSession): Map[String, ExecutionParameters[_, _]]

  /**
    * Loads a set of parameters needed for the ETL Operation
    *
    * through : config files loading, argument parsing, execution parameters creation, etc...
    *
    * @param args The list of arguments to parse
    * @return An ExecutionData object containing all the required parameters
    */
  def loadExecutionData(args: Array[String]): ExecutionData[_, _, _, _] = {
    for {
      /** -
        * Loading configuration files
        */
      loadedConfigurations <- ConfigurationsLoader(configDefinition)

      /**
        * Arguments parsing
        */
      parsedBaseArgument <- {
        SparkApplication.parseArguments(
          loadedConfigurations.sparkParamsConf.appName,
          args,
          DefaultArgument(),
          new BaseArgumentParser[DefaultArgument]
        )
      }

      /**
        * Loading job conf
        */
      jobConf <- ConfigsExtractor.getJob(parsedBaseArgument.table, loadedConfigurations.jobsConf)

      /**
        * Loading execution parameters
        */
      executionParameters <- {
        implicit val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)
        val jobName = parsedBaseArgument.table
        MonadUtils.optionToEither(executionPlans.get(jobName), ExecutionPlanNotFoundError(jobName))
      }
    } yield {
      /**
        * Optional custom arguments
        */
      val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)
      val parsedCustomArguments: Option[_] = executionParameters.additionalArguments.map {
        case ArgumentData(defaultArgument, argumentSequence) =>
          handleEither(
            SparkApplication.parseArguments(
              loadedConfigurations.sparkParamsConf.appName,
              args,
              defaultArgument,
              argumentSequence
            )
          )
      }

      /**
        * Optional Application config
        */
      val applicationConf = configDefinition.applicationConf.map(conf =>
        handleEither(loadConfig(conf.configName, conf.either))
      )

      logger.info("Successfully loaded parameters from configuration files")
      ExecutionData(
        loadedConfigurations,
        parsedBaseArgument,
        OptionalJobParameters(applicationConf, parsedCustomArguments),
        executionParameters.executionFunction,
        jobConf,
        spark
      )
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
                         exportDateColumn: Option[String])(implicit spark: SparkSession): DataFrame = {

    def getSource(sourceName: String): EitherRP = {
      MonadUtils.optionToEither(sources.get(sourceName), JobSourcesNotFoundError(jobName, sourceName))
    }

    def attachExportDate: DataFrame => DataFrame = dataFrame => {
      exportDateColumn match {
        case Some(column) => dataFrame.withColumn(column, lit(DateUtils.today(datePattern.pattern)))
        case None => dataFrame
      }
    }

    attachExportDate(executionPlan
      .getExecutionPlan(getSource)
      .info("Successfully loaded Execution plan for data transformation")
      .transform)
  }

}

object SparkApplication {

  sealed trait OptionalData

  case class ApplicationConfData[T](configName: String, either: Either[ConfigReaderFailures, T]) extends OptionalData

  case class ArgumentData[T](defaultArgument: T, argumentSequence: ArgumentsDefinition[T]*) extends OptionalData

  private def parseArguments[U](appName: String, args: Array[String], defaultArgument: U, parsers: ArgumentsDefinition[U]*): Either[ParseArgumentsError, U] = {
    val argumentOption: Option[U] = OParser.parse(
      ArgumentsDefinition.buildParser(appName, parsers: _*),
      args,
      defaultArgument,
      new ParserSetup()
    )
    MonadUtils.optionToEither(argumentOption, ParseArgumentsError())
  }
}
