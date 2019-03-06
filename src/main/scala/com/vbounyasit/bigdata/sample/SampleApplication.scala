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

package com.vbounyasit.bigdata.sample

import com.vbounyasit.bigdata.ETL.{ExecutionParameters, OptionalJobParameters}
import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.SparkApplication.ArgumentData
import com.vbounyasit.bigdata.args.timefilter.TimeFilterArgumentParser
import com.vbounyasit.bigdata.config.ConfigDefinition
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.sample.data.{SampleApplicationConf, SampleArgument}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SampleApplication extends SparkApplication[SampleApplicationConf, SampleArgument] {

  override val configDefinition: ConfigDefinition = new SampleConfigDefinition

  override def executionPlans(implicit spark: SparkSession): Map[String, ExecutionParameters[_, _]] = Map(
    "job1" -> ExecutionParameters(
      SampleExecutionPlan(_: OptionalJobParameters[SampleApplicationConf, SampleArgument]),
      ArgumentData(
        SampleArgument(),
        new TimeFilterArgumentParser[SampleArgument](datePattern)
      )
    )
  )

  def main(args: Array[String]): Unit = {
    val executionData = loadExecutionData(args)
    runETL(executionData)
  }

  override def load(dataFrame: DataFrame,
                    database: String,
                    table: String,
                    optionalJobParameters: OptionalJobParameters[SampleApplicationConf, SampleArgument]): Unit = {
    dataFrame.show()
  }
}
