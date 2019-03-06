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

import com.vbounyasit.bigdata.transform.TransformComponents.Transformers
import com.vbounyasit.bigdata.transform.Transformer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class SampleTransformers(implicit spark: SparkSession) extends Transformers {

  case class Selector(columns: String*) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.select(columns.map(col): _*)
  }

  case class DropDuplicates(columns: String*) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.dropDuplicates(columns)
  }

}


