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

package com.vbounyasit.bigdata.testing.formats

import java.io.File

import com.vbounyasit.bigdata.exceptions.ExceptionHandler.{CorruptedResourceFile, ReadDataFramesFromFilesError, ResourcesDataFileNotFound}
import com.vbounyasit.bigdata.testing.data.JobTableMetadata
import com.vbounyasit.bigdata.utils.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Classes related to any DataFrame I/O operations for a given file format
  */
object DataFrameIO {

  abstract class DataFrameLoader(fileExtension: String) {

    /**
      * Builds the exception object for a file that is not found
      *
      * @param jobTableMetadata the Job table metadata
      * @return the built resource not found exception
      */
    final def getFileNotFoundException(jobTableMetadata: JobTableMetadata): ResourcesDataFileNotFound = {
      import jobTableMetadata._
      ResourcesDataFileNotFound(jobName, s"$database.$table.$fileExtension")
    }

    /**
      * Loads DataFrames from Resource files for a given job
      *
      * @param jobTablesMetadata the list of sources
      * @return A Map of sourceInfo/DataFrame values
      */
    def loadDataFrames(jobTablesMetadata: Seq[JobTableMetadata], ioType: String)(implicit spark: SparkSession): List[Either[ReadDataFramesFromFilesError, (JobTableMetadata, DataFrame)]]


    /**
      * Reads a json file into a DataFrame
      *
      * @param jobTableMetadata the jobSource info
      * @param spark            the implicit spark session
      * @return the converted json to DataFrame
      */
    protected final def readDfFromFile(jobTableMetadata: JobTableMetadata, ioType: String)(implicit spark: SparkSession): Either[ReadDataFramesFromFilesError, (JobTableMetadata, Option[DataFrame])] = {
      import jobTableMetadata._
      val folderPath = s"${IOUtils.resourcesPath}/jobs/$jobName/$ioType"
      val fileName = s"$database.$table.$fileExtension"

      val filePath = s"$folderPath/$fileName"
      val files = Option(new File(folderPath).listFiles())
      val filePresent = files.nonEmpty && files.get.map(_.getName).contains(fileName)

      if (filePresent) {
        val readDataFrame: DataFrame = convertFileToDataFrame(filePath)
        val dataFrameOption: Option[DataFrame] = if (!readDataFrame.columns.head.contains("corrupt_record")) Some(readDataFrame) else None
        Either.cond(dataFrameOption.isDefined, (jobTableMetadata, dataFrameOption), CorruptedResourceFile(jobName, fileName))
      } else {
        Right(jobTableMetadata, None)
      }
    }


    /**
      * Converts a given resource file to a DataFrame
      *
      * @param filePath the path to the file
      * @param spark    an implicit spark session
      * @return the resulting DataFrame
      */
    protected def convertFileToDataFrame(filePath: String)(implicit spark: SparkSession): DataFrame


  }


  abstract class DataFrameWriter(fileExtension: String) {

    /**
      * Writes a dataframe into a file
      *
      * @param jobName   the job name
      * @param database  the database name
      * @param table     the table name
      * @param dataFrame the dataFrame to serialize
      */
    final def writeDataFrameToResources(jobName: String, database: String, table: String, ioType: String, dataFrame: DataFrame): Unit = {
      val filePath = s"jobs/$jobName/$ioType/$database.$table.json"
      IOUtils.writeToResources(dataFrameStringConversion(dataFrame), filePath)
    }

    /**
      * The string obtained from a given DataFrame to write to resources
      *
      * @param dataFrame the DataFrame to convert
      * @return The converted String
      */
    protected def dataFrameStringConversion(dataFrame: DataFrame): String

  }

}
