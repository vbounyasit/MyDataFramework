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

import com.vbounyasit.bigdata.ETL.OptionalJobParameters
import com.vbounyasit.bigdata.sample.data.{SampleApplicationConf, SampleArgument}
import com.vbounyasit.bigdata.transform.TransformComponents._
import com.vbounyasit.bigdata.transform.pipeline.impl.Pipeline
import org.apache.spark.sql.SparkSession

class SampleExecutionPipelines(optionalParams: OptionalJobParameters[SampleApplicationConf, SampleArgument])(implicit spark: SparkSession) extends ExecutionPipelines {

  override val transformers: SampleTransformers = new SampleTransformers

  import transformers._

  val pipeline1 = Pipeline(
    Selector("col3", "col2", "col1")
  )

}
