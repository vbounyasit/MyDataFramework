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

package com.vbounyasit.bigdata.providers

import com.vbounyasit.bigdata.config.data.SparkParamsConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Vayken on 4/2/2018.
  *
  * This is for local testing environment.
  * The job is supposed to connect to the cluster
  */
trait SparkSessionProvider {

  final def getSparkSession(sparkParamsConf: SparkParamsConf): SparkSession = {
    def getSparkConf(sparkParamsConf: SparkParamsConf): SparkConf = {
      sparkParamsConf.params.foldLeft(new SparkConf()) {
        case (sparkConf, (key, value)) =>
          sparkConf.set(key.replace("-", "."), value)
      }
    }

    val spark = SparkSession.builder()
      .appName(sparkParamsConf.appName)
      .master(sparkParamsConf.master)
      .config(getSparkConf(sparkParamsConf))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel(sparkParamsConf.logLevel)
    spark
  }
}
