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

package com.vbounyasit.bigdata

import com.vbounyasit.bigdata.ETL._
import com.vbounyasit.bigdata.args.ArgumentsConfiguration
import com.vbounyasit.bigdata.config.data.JobsConfig.{JobConf, JobSource}
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.config.{ConfigurationsLoader, CustomConfig}
import com.vbounyasit.bigdata.transform.ExecutionPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * A trait defining the functions for an ETL operation.
  */
trait ETL {

  protected def parseApplicationParameters(args: Array[String]): ParsedParameters[_, _]

  /**
    * Loads a set of parameters needed for the ETL Operation
    *
    * through : config files loading, argument parsing, execution parameters creation, etc...
    *
    * @param args The list of arguments to parse
    * @return An ExecutionData object containing all the required parameters
    */
  protected def loadExecutionData(configuration: ConfigurationsLoader,
                                  tablesToCompute: Seq[TableMetadata],
                                  environment: String,
                                  args: Array[String]): ExecutionData

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
  def extract(jobName: String,
              jobSourcesConf: List[JobSource],
              sourcesConf: SourcesConf,
              env: String)(implicit spark: SparkSession): Sources

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
  def transform(jobName: String,
                sources: Sources,
                executionPlan: ExecutionPlan,
                outputColumns: Option[Seq[String]],
                exportDateColumn: Option[String])(implicit spark: SparkSession): DataFrame

  /**
    * Saves the resulting dataFrame to disk
    *
    * @param dataFrame             The resulting DataFrame
    * @param outputTable           The output database and table
    * @param inputParameters       A ParametersPair object containing the global input parameters.
    */
  def load[GlobalConfig, GlobalArgument, Config, Argument](dataFrame: DataFrame,
           outputTable: TableMetadata,
           inputParameters: InputParameters[GlobalConfig, GlobalArgument, Config, Argument]): Unit


  /**
    * The main method containing the logic for running our ETL job
    *
    * @param executionData The ExecutionData object that will be used
    */
  def runETL[GlobalConfig, GlobalArgument, Config, Argument, ConfigInput, ArgumentInput](parsedParameters: ParsedParameters[GlobalConfig, GlobalArgument],
                                                                                         executionData: ExecutionData): Unit = {
    implicit val spark: SparkSession = executionData.spark
    type JobExecParams = JobExecutionParameters[GlobalConfig, GlobalArgument, Config, Argument, ConfigInput, ArgumentInput]

    val globalParameters: ParametersPair[GlobalConfig, GlobalArgument] = ParametersPair(parsedParameters.applicationConf, parsedParameters.applicationArguments)

    executionData.jobExecutionParameters
      .foreach(jobParametersExistential => {
        val jobExecutionParameters: JobExecParams = jobParametersExistential.asInstanceOf[JobExecParams]
        val jobParameters = ParametersPair(
          jobExecutionParameters.jobParameters.applicationConfig.map(_.asInstanceOf[ConfigInput]),
          jobExecutionParameters.jobParameters.arguments.map(_.asInstanceOf[ArgumentInput])
        )
        val inputParameters = InputParameters(
          globalParameters,
          jobParameters
        )
        //extract
        val sources: Sources = extract(
          jobExecutionParameters.outputTable.table,
          jobExecutionParameters.jobConf.sources,
          parsedParameters.configurations.sourcesConf,
          executionData.environment)

        //transform
        val resultDataFrame = transform(
          jobExecutionParameters.outputTable.table,
          sources,
          jobExecutionParameters.executionFunction(
            inputParameters
          ),
          Some(jobExecutionParameters.jobConf.outputMetadata.outputColumns),
          Some(jobExecutionParameters.jobConf.outputMetadata.dateColumn)
        )

        //load
        load(
          resultDataFrame,
          jobExecutionParameters.outputTable,
          inputParameters
        )
      })
  }
}

/**
  * The different data classes used in our ETL operation.
  */
object ETL {

  type ExecutionConfig = ExecutionConfigs[_, _, _, _]

  type EmptyJobParameters = ParametersPair[Nothing, Nothing]

  case class ParsedParameters[GlobalConfig, GlobalArgument](configurations: ConfigurationsLoader,
                                                            applicationConf: Option[GlobalConfig],
                                                            applicationArguments: Option[GlobalArgument])

  case class ExecutionData(jobExecutionParameters: Seq[JobExecutionParameters[_, _, _, _, _, _]],
                           spark: SparkSession,
                           environment: String)

  case class JobExecutionParameters[GlobalConfig, GlobalArgument, Config, Argument, ConfigInput, ArgumentInput](jobConf: JobConf,
                                                                                                                outputTable: TableMetadata,
                                                                                                                jobParameters: ParametersPair[Config, Argument],
                                                                                                                executionFunction: InputParameters[GlobalConfig, GlobalArgument, ConfigInput, ArgumentInput] => ExecutionPlan)

  case class TableMetadata(database: String, table: String)

  case class ParametersPair[Config, Argument](applicationConfig: Option[Config],
                                              arguments: Option[Argument])

  case class InputParameters[GlobalConfig, GlobalArgument, Config, Argument](globalParameter: ParametersPair[GlobalConfig, GlobalArgument],
                                                                             jobSpecificParameter: ParametersPair[Config, Argument])


  case class ExecutionConfigs[GlobalConfig, GlobalArgument, Config, Argument](executionFunction: InputParameters[GlobalConfig, GlobalArgument, Config, Argument] => ExecutionPlan,
                                                                              additionalConfig: Option[CustomConfig[Config]] = None,
                                                                              additionalArguments: Option[ArgumentsConfiguration[Argument]] = None)


  object ExecutionConfigs {
    def apply[GlobalConfig, GlobalArgument, Config, Argument](executionPlan: ExecutionPlan): ExecutionConfigs[GlobalConfig, GlobalArgument, Config, Argument] =
      ExecutionConfigs(_ => executionPlan)
  }

}
