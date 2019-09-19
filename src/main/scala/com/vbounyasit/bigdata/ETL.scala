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

import com.vbounyasit.bigdata.ETL.{ExecutionData, OptionalJobParameters}
import com.vbounyasit.bigdata.args.ArgumentsConfiguration
import com.vbounyasit.bigdata.args.base.OutputArguments
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
  def loadExecutionData(args: Array[String]): ExecutionData[_, _, _, _]

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
  def runETL[Config, Argument, ConfigInput, ArgumentInput](executionData: ExecutionData[Config, Argument, ConfigInput, ArgumentInput]): Unit = {

    implicit val spark: SparkSession = executionData.spark

    //extract
    val sources: Sources = extract(
      executionData.baseArguments.table,
      executionData.jobConf.sources,
      executionData.configurations.sourcesConf,
      executionData.baseArguments.env)

    //transform
    val resultDataFrame = transform(
      executionData.baseArguments.table,
      sources,
      //relying on runtime for this cast
      executionData.executionFunction(
        OptionalJobParameters(
          executionData.optionalJobParameters.applicationConfig.map(_.asInstanceOf[ConfigInput]),
          executionData.optionalJobParameters.arguments.map(_.asInstanceOf[ArgumentInput])
        )
      ),
      Some(executionData.jobConf.outputMetadata.dateColumn)
    )
    //load
    load(
      resultDataFrame,
      executionData.baseArguments.database,
      executionData.baseArguments.table,
      executionData.optionalJobParameters.asInstanceOf[OptionalJobParameters[U, V]]
    )
  }
}

/**
  * The different data classes used in our ETL operation.
  */
object ETL {

  case class ExecutionData[Config, Argument, ConfigInput, ArgumentInput](configurations: ConfigurationsLoader,
                                                                         baseArguments: OutputArguments,
                                                                         optionalJobParameters: OptionalJobParameters[Config, Argument],
                                                                         executionFunction: OptionalJobParameters[ConfigInput, ArgumentInput] => ExecutionPlan,
                                                                         jobConf: JobConf,
                                                                         spark: SparkSession)

  case class OptionalJobParameters[Config, Argument](applicationConfig: Option[Config],
                                                     arguments: Option[Argument])

  case class ExecutionParameters[Config, Argument](executionFunction: OptionalJobParameters[Config, Argument] => ExecutionPlan,
                                                   additionalArguments: Option[ArgumentsConfiguration[Argument]] = None)


  object ExecutionParameters {
    def apply[Config, Argument](executionPlan: ExecutionPlan): ExecutionParameters[Config, Argument] =
      ExecutionParameters(_ => executionPlan)
  }

}
