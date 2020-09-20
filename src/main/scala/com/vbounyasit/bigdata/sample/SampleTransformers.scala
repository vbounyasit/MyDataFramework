/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 4:19 AM
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

import com.vbounyasit.bigdata.transform.TransformComponents.Transformers
import com.vbounyasit.bigdata.transform.Transformer
import org.apache.spark.sql.DataFrame

class SampleTransformers extends Transformers {
  import org.apache.spark.sql.functions._

  case class FilterOnTable1() extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.where(col("col3") === "text2")
  }

  case class MultiplyByFactor(columnName: String, factor: Int) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumn(columnName, col(columnName) * factor)
  }

  case class MultiplyByColumn(columnName: String, multiplyByColumn: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumn(columnName, col(columnName) * col(multiplyByColumn))
  }

  case class SumByColumn(keyColumn: String, columnToSum: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.groupBy(keyColumn).agg(sum(columnToSum).as(s"sum_of_$columnToSum"))
  }

  case class KeepGreaterThan(columnName: String, thresholdValue: Int) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.where(col(columnName) >= thresholdValue)
  }

  case class RenameColumn(columnName: String, newColumnName: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumnRenamed(columnName, newColumnName)
  }
}