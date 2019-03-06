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

package com.vbounyasit.bigdata

import com.vbounyasit.bigdata.ETL.OptionalJobParameters
import com.vbounyasit.bigdata.sample.SampleExecutionPipelines
import com.vbounyasit.bigdata.sample.data.SampleArgument
import com.vbounyasit.bigdata.testing.UnitTestDefinition
import com.vbounyasit.bigdata.testing.data.UnitTestDefinition._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

class SampleUnitTestDefinition(implicit spark: SparkSession) extends UnitTestDefinition {

  override val executionPipelines: SampleExecutionPipelines =
    new SampleExecutionPipelines(
      OptionalJobParameters(
        None,
        Some(SampleArgument("2019-02-28", "2019-02-28"))
      )
    )

  import executionPipelines.transformers._

  override val unitTests: Seq[UnitTest] = Seq(
    UnitTest("Selector",
      Selector("col1", "col2"),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col2", StringType),
          ("col3", StringType)
        ),
        Row(5, "col2value", "col3value"),
        Row(55, "col2value1", "col3value1"),
        Row(15, "col2value2", "col3value2"),
        Row(20, "col2value3", "col3value3")
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col2", StringType)
        ),
        Row(5, "col2value"),
        Row(55, "col2value1"),
        Row(15, "col2value2"),
        Row(20, "col2value3")
      )
    )
  )
}
