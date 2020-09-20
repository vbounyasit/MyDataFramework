/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 5:06 AM
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

import com.vbounyasit.bigdata.transform.TransformComponents.ExecutionPipelines
import com.vbounyasit.bigdata.transform.pipeline.Pipeline

class SampleExecutionPipelines extends ExecutionPipelines {

  override val transformers: SampleTransformers = new SampleTransformers

  import transformers._

  val table1Pipeline1: Pipeline = Pipeline(
    FilterOnTable1()
  )
  val table1Pipeline2: Pipeline = Pipeline(
    MultiplyByFactor("col1", 5),
    MultiplyByFactor("col2", 2)
  )
  val table2Pipeline: Pipeline = Pipeline(
    MultiplyByColumn("col33", "col11"),
    MultiplyByColumn("col33", "col22")
  )
  val table2AggregationPipeline: Pipeline = Pipeline(
    SumByColumn("index_col", "col33")
  )
  val postJoinPipeline: Pipeline = Pipeline(
    MultiplyByColumn("sum_of_col33", "col1"),
    RenameColumn("sum_of_col33", "result"),
    KeepGreaterThan("result", 2000000)
  )
}

