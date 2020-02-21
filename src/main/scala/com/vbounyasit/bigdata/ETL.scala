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

import com.vbounyasit.bigdata.ETL.{ExecutionData, JobFullExecutionParameters, OptionalJobParameters}
import com.vbounyasit.bigdata.args.ArgumentsConfiguration
import com.vbounyasit.bigdata.config.ConfigurationsLoader
import com.vbounyasit.bigdata.config.data.JobsConfig.{JobConf, JobSource}
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.transform.ExecutionPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * A trait defining the functions for an ETL operation.
  */
trait ETL[U, V] {

  /**
    * Loads a set of parameters needed for the ETL Operation
    *
    * through : config files loading, argument parsing, execution parameters creation, etc...
    *
    * @param args The list of arguments to parse
    * @return An ExecutionData object containing all the required parameters
    */
  protected def loadExecutionData(args: Array[String]): ExecutionData[_, _]

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
    * @param database              The output database name
    * @param table                 The output table name (job name)
    * @param optionalJobParameters An OptionalJobParameters object containing any custom
    *                              argument/application files we defined through our application.
    */
  def load(dataFrame: DataFrame,
           database: String,
           table: String,
           optionalJobParameters: OptionalJobParameters[U, V]): Unit


  /**
    * The main method containing the logic for running our ETL job
    *
    * @param executionData The ExecutionData object that will be used
    */
  def runETL[GlobalConfig, GlobalArgument, Config, Argument, ConfigInput, ArgumentInput](executionData: ExecutionData[GlobalConfig, GlobalArgument]): Unit = {
    implicit val spark: SparkSession = executionData.spark

    val globalApplicationConf: Option[GlobalConfig] = executionData.applicationConf
    val globalArguments: Option[GlobalArgument] = executionData.applicationArguments

    executionData.jobFullExecutionParameters
      .foreach(jobParametersExistential => {
        val jobParameters = jobParametersExistential.asInstanceOf[JobFullExecutionParameters[GlobalConfig, GlobalArgument, Config, Argument, ConfigInput, ArgumentInput]]
        //extract
        val sources: Sources = extract(
          jobParameters.outputTable.table,
          jobParameters.jobConf.sources,
          executionData.configurations.sourcesConf,
          executionData.environment)

        //transform
        val resultDataFrame = transform(
          jobParameters.outputTable.table,
          sources,
          //relying on runtime for this cast
          jobParameters.executionFunction(
            OptionalJobParameters(
              globalApplicationConf,
              globalArguments
            ),
            OptionalJobParameters(
              jobParameters.optionalJobParameters.applicationConfig.map(_.asInstanceOf[ConfigInput]),
              jobParameters.optionalJobParameters.arguments.map(_.asInstanceOf[ArgumentInput])
            )
          ),
          Some(jobParameters.jobConf.outputMetadata.outputColumns),
          Some(jobParameters.jobConf.outputMetadata.dateColumn)
        )
        //load
        load(
          resultDataFrame,
          jobParameters.outputTable.database,
          jobParameters.outputTable.table,
          jobParameters.optionalJobParameters.asInstanceOf[OptionalJobParameters[U, V]]
        )
      })
  }
}

/**
  * The different data classes used in our ETL operation.
  */
object ETL {

  type ExecutionConfig = ExecutionParameters[_, _, _, _]

  type EmptyOptionalParameters = OptionalJobParameters[Nothing, Nothing]

  type OptionalParameters[GlobalConfig, GlobalArgument, Config, Argument] = (OptionalJobParameters[GlobalConfig, GlobalArgument], OptionalJobParameters[Config, Argument])

  case class ExecutionData[GlobalConfig, GlobalArgument](configurations: ConfigurationsLoader,
                                                         applicationConf: Option[GlobalConfig],
                                                         applicationArguments: Option[GlobalArgument],
                                                         jobFullExecutionParameters: Seq[JobFullExecutionParameters[_, _, _, _, _, _]],
                                                         spark: SparkSession,
                                                         environment: String)

  case class JobFullExecutionParameters[GlobalConfigInput, GlobalArgumentInput, Config, Argument, ConfigInput, ArgumentInput](jobConf: JobConf,
                                                                                                                              outputTable: TableMetadata,
                                                                                                                              optionalJobParameters: OptionalJobParameters[Config, Argument],
                                                                                                                              executionFunction: OptionalParameters[GlobalConfigInput, GlobalArgumentInput, ConfigInput, ArgumentInput] => ExecutionPlan)

  case class OptionalJobParameters[Config, Argument](applicationConfig: Option[Config],
                                                     arguments: Option[Argument])

  case class ExecutionParameters[GlobalConfig, GlobalArgument, Config, Argument](executionFunction: OptionalParameters[GlobalConfig, GlobalArgument, Config, Argument] => ExecutionPlan,
                                                   additionalArguments: Option[ArgumentsConfiguration[Argument]] = None)

  case class TableMetadata(database: String, table: String)

  object ExecutionParameters {
    def apply[GlobalConfig, GlobalArgument, Config, Argument](executionPlan: ExecutionPlan): ExecutionParameters[GlobalConfig, GlobalArgument, Config, Argument] =
      ExecutionParameters(_ => executionPlan)
  }

}
