/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 4:10 AM
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
import com.vbounyasit.bigdata.EitherRP
import com.vbounyasit.bigdata.sample.data.{SampleAppConf, SampleArgs}
import com.vbounyasit.bigdata.sample.joiner.SampleJoiner
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.transform.TransformOps._
import org.apache.spark.sql.SparkSession

class SampleExecutionPlan(optionalParameters: OptionalJobParameters[SampleAppConf, SampleArgs])(implicit spark: SparkSession) extends ExecutionPlan {

  override val executionPipelines: SampleExecutionPipelines = new SampleExecutionPipelines

  import executionPipelines._
  import executionPipelines.transformers._

  override def getExecutionPlan(getSource: String => EitherRP): EitherRP = {
    val source1Pipeline = getSource("source1") ==> table1Pipeline1 ==> table1Pipeline2
    val source2Pipeline = getSource("source2") ==> table2Pipeline ==> table2AggregationPipeline

    val result = source1Pipeline.join(source2Pipeline, new SampleJoiner)
    result ==> postJoinPipeline
  }
}
