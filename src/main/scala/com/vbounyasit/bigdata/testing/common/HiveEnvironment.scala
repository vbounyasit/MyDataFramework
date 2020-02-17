/*
 * Developed by Vibert Bounyasit
 * Last modified 4/7/19 7:15 PM
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

package com.vbounyasit.bigdata.testing.common

import cats.data.Validated
import cats.implicits._
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.config.ConfigsExtractor
import com.vbounyasit.bigdata.config.data.JobsConfig.JobsConf
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.{JobSourcesNotFoundError, ReadDataFramesFromFilesError}
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.providers.LoggerProvider
import com.vbounyasit.bigdata.testing.data.JobTableMetadata
import com.vbounyasit.bigdata.testing.environment
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * A class defining everything related to loading the local Hive environment
  *
  * @param dataFrameLoader The DataFrameLoader representing the files format we want to load our dataFrames from
  */
class HiveEnvironment(val dataFrameLoader: DataFrameLoader) extends LoggerProvider {

  /**
    * Setups the Hive environment containing the databases and tables for all the jobs
    *
    * @param sourcesConf the source configuration
    */
  def setupEnvironment(jobsConf: JobsConf, sourcesConf: SourcesConf, env: String = environment)(implicit spark: SparkSession): Unit = {
    Try {
      logger.info("Creating the required databases.")
      sourcesConf.databases.flatMap {
        case (_, dbInfo) => dbInfo.env.values
      }.foreach(db => spark.sql(s"CREATE DATABASE $db"))
    } match {
      case Success(_) => logger.info("Databases have been created.")
      case Failure(_) => logger.info("Databases already exist. Skipping...")
    }

    val jobSourcesInfo: List[Validated[JobSourcesNotFoundError, JobTableMetadata]] = jobsConf.jobs.flatMap {
      case (jobName, jobConf) =>
        jobConf.sources.map(jobSource => {
          ConfigsExtractor.matchJobSourceWithSources(jobName, jobSource, sourcesConf, environment) {
            case (database, table) => JobTableMetadata(jobName, database, table)
          }
        })
    }.toList

    val validatedJobSourcesInfo: Validated[JobSourcesNotFoundError, List[JobTableMetadata]] = jobSourcesInfo.sequence

    val dataFramesInfo = dataFrameLoader.loadDataFrames(validatedJobSourcesInfo.toEither, "in")
    val validatedDataFramesInfo: Either[ReadDataFramesFromFilesError, List[(JobTableMetadata, DataFrame)]] = dataFramesInfo.sequence

    validatedDataFramesInfo.foreach {
      case (jobSourceInfo, dataFrame) =>
        val info = Seq(s"${jobSourceInfo.database}", s"${jobSourceInfo.jobName}", s"${jobSourceInfo.table}")
        //adding in a job identifier, since we don't want conflict in jobs sharing common source tables
        val (viewName, tableName) = (s"${info.mkString("_")}", s"${info.head}.${info.tail.mkString("_")}")
        dataFrame.createOrReplaceTempView(viewName)
        Try(spark.sql(s"drop table if exists $tableName")) match {
          case Success(_) => logger.info(s"Successfully dropped table $tableName.")
          case Failure(_) => logger.info(s"Failed to drop table $tableName - table doesn't exist.")
        }
        spark.sql(s"create table $tableName as select * from $viewName")
        spark.sql(s"drop view if exists $viewName")
    }
    logger.info("Tables have been created.")
  }
}
