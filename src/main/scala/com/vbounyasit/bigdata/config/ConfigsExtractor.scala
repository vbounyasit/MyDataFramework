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

package com.vbounyasit.bigdata.config

import cats.data.Validated
import cats.implicits._
import com.vbounyasit.bigdata.Sources
import com.vbounyasit.bigdata.config.data.JobsConfig.{JobConf, JobSource, JobsConf}
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.{JobNotFoundError, JobSourcesDuplicatesFoundError, JobSourcesError, JobSourcesNotFoundError}
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.transform.TransformOps._
import com.vbounyasit.bigdata.transform.pipeline.impl.SourcePipeline
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Utility functions for extracting error-free data from config data
  */
object ConfigsExtractor {

  /**
    * Gets the sources from the conf files
    *
    * @param jobName        the Job Name
    * @param jobSourcesConf the job sources conf
    * @param sourcesConf    the global sources conf
    * @param env            the environment
    * @param spark          the implicit spark session
    * @return Either the resulting sources or a Job sources error exception
    */
  def getSources(jobName: String, jobSourcesConf: List[JobSource], sourcesConf: SourcesConf, env: String)(implicit spark: SparkSession): Either[JobSourcesError, Sources] = {
    val loadedSources: List[Validated[JobSourcesNotFoundError, (String, SourcePipeline)]] = jobSourcesConf.map(jobSource => {
      matchJobSourceWithSources(jobName, jobSource, sourcesConf, env) {
        case (database, table) =>
          val sourcePipeline = spark.read.table(s"$database.$table")
          val selector: DataFrame => DataFrame = _.select(jobSource.selectedColumns.map(col): _*)
          val dropDuplicate: DataFrame => DataFrame = _.dropDuplicates()
          (jobSource.sourceName, sourcePipeline ==> selector ==> dropDuplicate)
      }
    })
    val loadedSourcesSequence: Validated[JobSourcesNotFoundError, List[(String, SourcePipeline)]] = loadedSources.sequence
    for {
      matchCheckedSources <- loadedSourcesSequence.toEither.map(_.toMap)
      duplicatesCheckedSources <- {
        val duplicatedSources = matchCheckedSources.groupBy(_._1).mapValues(_.size).filter(_._2 > 1).keys
        Either.cond(duplicatedSources.isEmpty, matchCheckedSources, JobSourcesDuplicatesFoundError(jobName, duplicatedSources.toSeq: _*))
      }
    } yield duplicatesCheckedSources
  }

  /**
    * Gets a JobConf object with a provided jobName and check its coherence
    *
    * @param jobName  the Job name
    * @param jobsConf the Job configuration list
    * @return Either the wanted Job configuration or a Job not found error exception
    */
  def getJob(jobName: String, jobsConf: JobsConf): Either[JobNotFoundError, JobConf] = {
    val jobConf = jobsConf.jobs.get(jobName)
    Either.cond(jobConf.isDefined, jobConf.get, JobNotFoundError(jobName))
  }

  /**
    * Check a Job source existence in the global Sources conf
    *
    * @param jobName         a job name
    * @param jobSource       a Job source conf
    * @param sourcesConf     the global Source conf
    * @param env             the environnment
    * @param appliedFunction A function that turns the obtained (database, table) into the wanted result
    * @tparam T the type of the result we want
    * @return a Validated object representing our result or an error
    */
  def matchJobSourceWithSources[T](jobName: String,
                                   jobSource: JobSource,
                                   sourcesConf: SourcesConf,
                                   env: String)
                                  (appliedFunction: ((String, String)) => T): Validated[JobSourcesNotFoundError, T] = {

    val resultTableInfo: Option[(String, String)] = for {
      tableInfo <- sourcesConf.tables.get(jobSource.sourceName)
      databaseInfo <- sourcesConf.databases.get(tableInfo.databaseName)
      databaseName <- {
        val environment = if (env == "default") tableInfo.defaultEnv else env
        databaseInfo.env.get(environment)
      }
    } yield {
      (databaseName, tableInfo.table)
    }
    Validated.cond(resultTableInfo.isDefined, {
      appliedFunction(resultTableInfo.get)
    }, JobSourcesNotFoundError(jobName, jobSource.sourceName))
  }


}
