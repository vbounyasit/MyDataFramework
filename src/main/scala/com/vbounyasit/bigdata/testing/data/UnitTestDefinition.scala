/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 7:24 PM
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

package com.vbounyasit.bigdata.testing.data

import com.vbounyasit.bigdata.transform.TransformOps._
import com.vbounyasit.bigdata.transform.Transformer
import com.vbounyasit.bigdata.transform.pipeline.impl.Pipeline
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Object containing all the data classes and implicit needed for unit testing
  */
object UnitTestDefinition {

  case class UnitTest(name: String,
                      transformer: TransformationDef,
                      inputDf: DataFrameDef,
                      outputDf: DataFrameDef)

  case class DataFrameSchema(fields: StructField*)

  case class DataFrameDef(schema: DataFrameSchema,
                          rows: Row*)

  implicit class TransformationDef(val pipeline: Pipeline)

  implicit def toStructField(info: (String, DataType)): StructField =
    StructField(info._1, info._2, nullable = true)

  implicit def toStructField(info: (String, DataType, Boolean)): StructField =
    StructField(info._1, info._2, info._3)

  implicit def toDataFrame(dataFrameDef: DataFrameDef)(implicit spark: SparkSession): DataFrame = {
    val rowRDD = spark.sparkContext.parallelize(dataFrameDef.rows)
    spark.createDataFrame(rowRDD, new StructType(dataFrameDef.schema.fields.toArray))
  }

  object TransformationDef {
    implicit def apply(transformer: Transformer): TransformationDef = {
      new TransformationDef(transformer: Pipeline)
    }

    implicit def apply(function: DataFrame => DataFrame): TransformationDef = {
      new TransformationDef(function: Pipeline)
    }

    implicit def apply(transformers: Transformer*): TransformationDef = {
      new TransformationDef(transformers: Pipeline)
    }
  }
}
