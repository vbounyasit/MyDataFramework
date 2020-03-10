/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 5:34 PM
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

package com.vbounyasit.bigdata.testing.formats.json

import com.vbounyasit.bigdata.exceptions.ErrorHandler.ReadDataFramesFromFilesError
import com.vbounyasit.bigdata.testing.data.JobTableMetadata
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonDfLoader extends DataFrameLoader("json") {


  /**
    * Loads DataFrames from Resource files for a given job
    * If the files required aren't present, then they will be outed from the list
    * If the files are present, but corrupted, they will return an error as Left(exception)
    * Otherwise, they will be in the list as Right(result)
    *
    * @param jobSourceInfos the list of sources
    * @return A Map of sourceInfo/DataFrame values
    */
  override def loadDataFrames(jobSourceInfos: Seq[JobTableMetadata], ioType: String)(implicit spark: SparkSession): List[Either[ReadDataFramesFromFilesError, (JobTableMetadata, DataFrame)]] = {
    jobSourceInfos
      .map(jobSourceInfo => {
        readDfFromFile(jobSourceInfo, ioType)
      })
      .filterNot(either => either.isRight && either.right.get._2.isEmpty)
      .map(either => either.right.map {
        case (jobSourceInfo, dataFrameOption) => (jobSourceInfo, dataFrameOption.get)
      })
      .toList
  }


  /**
    * Converts a given resource file to a DataFrame
    *
    * @param filePath the path to the file
    * @param spark    an implicit spark session
    * @return the resulting DataFrame
    */
  override protected def convertFileToDataFrame(filePath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    val json: RDD[String] = spark.sparkContext.wholeTextFiles(filePath).values.map(_.replace("\n", "").trim)
    spark.read.json(json.toDS)
  }
}
