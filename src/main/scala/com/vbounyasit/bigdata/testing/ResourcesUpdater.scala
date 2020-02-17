/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 5:32 PM
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

import java.io.File

import cats.data.Validated
import cats.implicits._
import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.appImplicits._
import com.vbounyasit.bigdata.config.data.JobsConfig.JobSource
import com.vbounyasit.bigdata.config.{ConfigsExtractor, ConfigurationsLoader}
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.{JobSourcesNotFoundError, ReadDataFramesFromFilesError}
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.providers.{LoggerProvider, SparkSessionProvider}
import com.vbounyasit.bigdata.testing.ResourcesUpdater.History
import com.vbounyasit.bigdata.testing.data.JobTableMetadata
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.{DataFrameLoader, DataFrameWriter}
import com.vbounyasit.bigdata.utils.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * This will update or create input files in a defined format that will
  * act as input sources for our job Integration test(s)
  */
abstract class ResourcesUpdater extends SparkSessionProvider with LoggerProvider {

  /**
    * The spark application to generate files for
    */
  val sparkApplication: SparkApplication[_, _]

  /**
    * Our DataFrame loader and writer instances
    */
  val dataFrameLoader: DataFrameLoader

  val dataFrameWriter: DataFrameWriter

  /**
    * Writing or updating source files in Resources folder
    *
    * @param sources the sources to update/create
    * @param spark   the implicit spark session
    * @return A sequence of history objects to be recorded
    */
  def updateSources(sources: Seq[(JobTableMetadata, JobSource)])(implicit spark: SparkSession): Seq[History] = {
    val loadedDataFramesInfo = dataFrameLoader.loadDataFrames(sources.map(_._1), "in")
    val validationDataFramesInfo: Either[ReadDataFramesFromFilesError, List[(JobTableMetadata, DataFrame)]] = loadedDataFramesInfo.sequence
    val dataFramesInfo: Map[JobTableMetadata, DataFrame] = validationDataFramesInfo.toMap

    def buildDiffMessage(columnsDiff: Set[String], action: String): Option[String] = {
      if (columnsDiff.nonEmpty)
        Some(s"\n\tcolumns $action : ${columnsDiff.mkString(",")}")
      else None
    }

    val sourcesInfo: Seq[(JobTableMetadata, DataFrame, History)] = sources.map {
      case (jobSourceInfo, jobSource) =>
        val (dataFrameToWrite, historyContent): (DataFrame, String) = dataFramesInfo.get(jobSourceInfo) match {
          case Some(dataFrame) =>
            val newColumns = jobSource.selectedColumns.toSet.diff(dataFrame.columns.toSet)
            val removedColumns = dataFrame.columns.toSet.diff(jobSource.selectedColumns.toSet)

            val newColsMessage = buildDiffMessage(newColumns, "added")
            val colsRemovedMessage = buildDiffMessage(removedColumns, "removed")

            val transformationsToApply: Seq[DataFrame => DataFrame] =
              newColumns.map(column => (df: DataFrame) => df.withColumn(column, lit(defaultColumnValue))).toSeq ++
                removedColumns.map(column => (df: DataFrame) => df.drop(column)).toSeq

            val newDataFrame = transformationsToApply.foldLeft(dataFrame)((acc, f) => f(acc))
            val logContent = newColsMessage |+| colsRemovedMessage

            (newDataFrame, logContent.getOrElse(none))
          case None => (createDefaultDataFrameFromConfigs(jobSource), s"Source created.")
        }
        (jobSourceInfo, dataFrameToWrite.select(jobSource.selectedColumns.map(col): _*), History(s"Sources Updates for [${jobSourceInfo.jobName}] ${jobSource.sourceName}", historyContent))
    }

    /**
      * Writing the DataFrames to the in directory
      */
    sourcesInfo.foreach {
      case (jobSourceInfo, dataFrameToWrite, _) =>
        import jobSourceInfo._
        dataFrameWriter.writeDataFrameToResources(jobName, database, table, "in", dataFrameToWrite)
    }

    /**
      * Deleting unused old sources
      */
    val filesDeletedHistory = sourcesInfo.map(_._1.jobName).distinct.map(jobName => {
      val filesToKeep = sourcesInfo.map {
        case (jobSourceInfo, _, _) => s"${jobSourceInfo.database}.${jobSourceInfo.table}"
      }
      val filesDeleted: Seq[String] = clearJobInputDirectory(jobName, filesToKeep)
      History(s"$jobName - Sources deleted", s"${filesDeleted.mkString(",")}")
    }).filter(_.content.nonEmpty)

    sourcesInfo.map(_._3) ++ filesDeletedHistory
  }

  /**
    * Main unit run method
    */
  def runResourceUpdates(env: String = environment): Unit = {
    val loadedConfigurations = ConfigurationsLoader(sparkApplication.configDefinition, useLocalSparkParams = true)

    implicit val spark: SparkSession = getSparkSession(loadedConfigurations.sparkParamsConf)

    val sourcesToUpdate: List[Validated[JobSourcesNotFoundError, (JobTableMetadata, JobSource)]] = loadedConfigurations.jobsConf.jobs.flatMap {
      case (jobName, jobConf) =>
        jobConf.sources.map(jobSource => {
          ConfigsExtractor.matchJobSourceWithSources(jobName, jobSource, loadedConfigurations.sourcesConf, env) {
            case (database, table) =>
              (JobTableMetadata(jobName, database, table), jobSource)
          }
        })
    }.toList
    val validatedSourcesToUpdate: Validated[JobSourcesNotFoundError, List[(JobTableMetadata, JobSource)]] = sourcesToUpdate.sequence
    val updateSourcesHistory: Seq[History] =
      updateSources(validatedSourcesToUpdate.toEither)
        .filter(!_.content.contains(none)) match {
        case Nil => Seq(History("History", "No updates so far"))
        case histories => histories
      }
    updateHistory(updateSourcesHistory)
  }

  /**
    * Creates a default DataFrame based on the job source data
    *
    * @param source the source to create a dataFrame from
    * @param spark  the implicit spark session
    * @return a DataFrame containing default rows and the right schema
    */
  def createDefaultDataFrameFromConfigs(source: JobSource)(implicit spark: SparkSession): DataFrame = {
    val schema: StructType = StructType(source.selectedColumns.map(column => StructField(column, defaultColumnType)))
    val defaultRow = Row(List.fill(source.selectedColumns.length)(defaultColumnValue): _*)
    val rdd: RDD[Row] = spark.sparkContext.parallelize(List.fill(defaultSourceRowCount)(defaultRow))
    spark.createDataFrame(rdd, schema)
  }

  /**
    * Clearing the job directory sources
    *
    * @param jobName       The job name
    * @param sourcesToKeep The sources to not delete
    * @return A sequence of file names to delete
    */
  private def clearJobInputDirectory(jobName: String, sourcesToKeep: Seq[String]): Seq[String] = {
    val path = s"${IOUtils.resourcesPath}/jobs/$jobName/in"
    val filesToDelete = new File(path).listFiles()
      .filter(file => !sourcesToKeep.exists(file.getName.contains(_)))
    IOUtils.deleteFiles(filesToDelete, path)
    filesToDelete.map(_.getName)
  }

  /**
    * Updates the history files for each operations, keeping a limited amount of records
    *
    * @param histories The sequence of History objects to persist
    */
  private def updateHistory(histories: Seq[History]): Unit = {
    val historyPath = "jobs/source_history.txt"
    val fullHistory = s"${histories.map(history => s"${history.title} : ${history.content}").mkString("\n")}$lineSeparator"
    IOUtils.readFromResources(historyPath) match {
      case Some(history) =>
        val loadedHistories = history.split(s"$lineSeparator")
        if (loadedHistories.size >= maxHistoryRecords) {
          IOUtils.writeToResources(s"$fullHistory${loadedHistories.dropRight(1).mkString(lineSeparator)}$lineSeparator", historyPath)
        } else {
          IOUtils.writeToResources(s"$fullHistory$history", historyPath)
        }
      case None =>
        IOUtils.writeToResources(s"$fullHistory", historyPath)
    }
    logger.info("\nSuccessfully updated resources. See source_history.txt for more information.")
  }
}

object ResourcesUpdater {

  case class History(title: String, content: String)

}