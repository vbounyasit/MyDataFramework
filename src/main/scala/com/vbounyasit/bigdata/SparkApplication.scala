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
import com.vbounyasit.bigdata.args.base.{OutputArguments, OutputArgumentsConf}
import com.vbounyasit.bigdata.config.ConfigurationsLoader.loadConfig
import com.vbounyasit.bigdata.config.data.JobsConfig.{JobConf, JobSource}
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.config.{ConfigDefinition, ConfigsExtractor, ConfigurationsLoader}
import com.vbounyasit.bigdata.exceptions.ExceptionHandler._
import com.vbounyasit.bigdata.providers.{LoggerProvider, SparkSessionProvider}
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.utils.{CollectionsUtils, DateUtils, MonadUtils}
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  protected def loadExecutionData(args: Array[String]): ExecutionData[_, _, _, _] = {

    case class TableMetadataSeq(tables: Seq[TableMetadata])

    case class JobsConfMap(configs: Map[String, JobConf])

    case class ExecutionParametersMap(parameters: Map[String, ExecutionConfig])

    handleEither(for {
      /**
        * Loading configuration files
        */
      loadedConfigurations <- ConfigurationsLoader(configDefinition)

      /**
        * Arguments parsing
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
        val output: Option[TableMetadataSeq] = configDefinition.resultingOutputTables match {
          case None => (parsedBaseArgument.table, parsedBaseArgument.database) match {
            case ("N/A", _) | (_, "N/A") => None
            case (database, table) => Some(TableMetadataSeq(Seq(TableMetadata(database, table))))
          }
          case resultingTables => resultingTables.map(TableMetadataSeq)
        }
        MonadUtils.optionToEither(output, NoOutputTablesSpecified())
      }

      /**
        * Loading jobs conf
        */
      jobsConf <- {
        ConfigsExtractor
          .getJobs(tablesToCompute.tables.map(_.table), loadedConfigurations.jobsConf)
          .map(JobsConfMap)
      }

      /**
        * Loading execution parameters
        */
      executionsParameters <- {
        implicit val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)
        MonadUtils
          .getMapSubList(tablesToCompute.tables.map(_.table).toList, executionPlans, ExecutionPlanNotFoundError)
          .map(ExecutionParametersMap)
      }
    } yield {
      val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)
      /**
        * Optional Application config
        * TODO : Currently, a single config file defined in ConfigDefinition will be used for every job's configuration. Might want to define one file per job.
        */
      val customConfiguration: Option[_] = configDefinition.applicationConf.map(conf =>
        handleEither(loadConfig(conf.configFileName, conf.pureConfigLoaded))
      )

      /**
        * Merging with the list of output tables
        */
      val jobsConfWithOutputMetadata: Map[String, (JobConf, TableMetadata)] = CollectionsUtils.mergeByKeyStrict(
        jobsConf.configs,
        tablesToCompute.tables.map(metadata => (metadata.table, metadata)).toMap,
        MergingMapKeyNotFound
      )

      /**
        * Building the final Job parameters object
        */
      val jobFullExecutionParameters = CollectionsUtils
        .mergeByKeyStrict(
          jobsConfWithOutputMetadata,
          executionsParameters.parameters,
          MergingMapKeyNotFound
        ).values
        /**
          * Adding Optional custom arguments
          */
        .map {
          case ((jobConf, tableMetadata), executionConfig) =>
            val parsedCustomArguments: Option[_] = executionConfig.additionalArguments.map(argsConf => {
              val argumentParser = argsConf.argumentParser
              handleEither(argumentParser.parseArguments(
                loadedConfigurations.sparkParamsConf.appName,
                args
              ))
            })
            JobFullExecutionParameters(jobConf, tableMetadata, OptionalJobParameters(customConfiguration, parsedCustomArguments), executionConfig.executionFunction)
        }.toSeq
      logger.info("Successfully loaded parameters from configuration files")

      ExecutionData(
        loadedConfigurations,
        jobFullExecutionParameters,
        spark,
        parsedBaseArgument.env
      )
    })
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
      MonadUtils.optionToEither(sources.get(sourceName), JobSourcesNotFoundError(jobName, sourceName))
    }

    def selectOutputColumns: DataFrame => DataFrame = dataFrame => {
      outputColumns match {
        case Some(columns) => dataFrame.select(columns.map(col): _*)
        case None => dataFrame
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

object SparkApplication {

  case class ApplicationConfData[T](configFileName: String, pureConfigLoaded: PureConfigLoaded[T])

}
