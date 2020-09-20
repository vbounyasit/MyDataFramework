/*
 * Developed by Vibert Bounyasit
 * Last modified 6/14/19 7:50 PM
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

import com.vbounyasit.bigdata.testing.UnitTestDefinition
import com.vbounyasit.bigdata.testing.data.UnitTestDefinition.{DataFrameDef, DataFrameSchema, UnitTest, _}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

class SampleUnitTestDefinition(implicit spark: SparkSession) extends UnitTestDefinition {

  override protected val executionPipelines: SampleExecutionPipelines = new SampleExecutionPipelines

  import executionPipelines.transformers._

  override val unitTests: Seq[UnitTest] = Seq(
    UnitTest(
      "Filter table 1",
      FilterOnTable1(),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col3", StringType)
        ),
        Row(5, "text2"),
        Row(15, "text2"),
        Row(205, "text2"),
        Row(545, "text1"),
        Row(545, "text3")
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col3", StringType)
        ),
        Row(5, "text2"),
        Row(15, "text2"),
        Row(205, "text2")
      )
    ),
    UnitTest(
      "Multiply column by factor",
      MultiplyByFactor("testCol", 10),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(5, 6),
        Row(7, 8),
        Row(9, 10),
        Row(0, 20)
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(50, 6),
        Row(70, 8),
        Row(90, 10),
        Row(0, 20)
      )
    ),
    UnitTest(
      "Multiply column by column",
      MultiplyByColumn("testCol", "testCol2"),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(5, 6),
        Row(7, 8),
        Row(9, 10),
        Row(0, 20)
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(35, 6),
        Row(56, 8),
        Row(90, 10),
        Row(0, 20)
      )
    ),
    UnitTest(
      "Sum by column",
      SumByColumn("keyCol", "testCol"),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("keyCol", IntegerType),
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(1, 5, 6),
        Row(1, 7, 8),
        Row(2, 9, 10),
        Row(2, 8, 10),
        Row(3, 0, 20),
        Row(3, 9, 4),
        Row(3, 9, 2),
        Row(4, 6, 30),
        Row(5, 9, 20)
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("keyCol", IntegerType),
          ("sum_of_testCol", IntegerType)
        ),
        Row(1, 12),
        Row(2, 17),
        Row(3, 18),
        Row(4, 6),
        Row(5, 9)
      )
    ),
    UnitTest(
      "Keep column with value greater than val",
      KeepGreaterThan("testCol", 5),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(4, 1),
        Row(2, 8),
        Row(9, 10),
        Row(0, 20),
        Row(6, 21),
        Row(8, 25)
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(9, 10),
        Row(6, 21),
        Row(8, 25)
      )
    ),
    UnitTest(
      "Rename column",
      RenameColumn("testCol", "newCol"),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("testCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(4, 1),
        Row(2, 8),
        Row(9, 10)
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("newCol", IntegerType),
          ("testCol2", IntegerType)
        ),
        Row(4, 1),
        Row(2, 8),
        Row(9, 10)
      )
    )
  )
}
