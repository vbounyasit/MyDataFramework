/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 1:47 PM
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

package com.vbounyasit.bigdata.sample

import com.vbounyasit.bigdata.ETL.{ExecutionConfigs, JobParameters, JobParametersPair, TableMetadata}
import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.config.ConfigDefinition
import com.vbounyasit.bigdata.sample.data.{SampleAppConf, SampleArgs}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SampleApplication extends SparkApplication[SampleAppConf, SampleArgs] {

  override val configDefinition: ConfigDefinition = new SampleConfigDefinition

  override def executionPlans(implicit spark: SparkSession): Map[String, ExecutionConfigs[_, _, SampleAppConf, SampleArgs]] = Map(
    "job1" -> ExecutionConfigs[SampleAppConf, SampleArgs, SampleAppConf, SampleArgs](
      params => new SampleExecutionPlan(params),
      additionalArguments = Some(new SampleArgumentConf)
    )
  )

  override def load(dataFrame: DataFrame,
                    outputTable: TableMetadata,
                    optionalJobParameters: JobParameters[SampleAppConf, SampleArgs]): Unit = {
    dataFrame.show()
  }

  def main(args: Array[String]): Unit = {
    //val executionData = loadExecutionData(args)
    //runETL(executionData)
  }
}
