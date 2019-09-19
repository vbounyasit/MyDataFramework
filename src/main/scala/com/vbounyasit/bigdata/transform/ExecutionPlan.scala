/*
 * Developed by Vibert Bounyasit
 * Last modified 4/7/19 7:15 PM
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

package com.vbounyasit.bigdata.transform

import com.vbounyasit.bigdata.EitherRP
import com.vbounyasit.bigdata.transform.TransformComponents.ExecutionPipelines
import org.apache.spark.sql.SparkSession

/**
  * A trait representing The execution plan of a specific job
  * It will define the transformation graph to follow for this job.
  */
abstract class ExecutionPlan(implicit spark: SparkSession) {

  /**
    * The pool of transformation pipelines we want to use in our plan.
    */
  val executionPipelines: ExecutionPipelines

  /**
    * The Execution plan used in our job.
    *
    * @param getSource A function that will get a job input source.
    * @return Either the execution plan or an Exception if the definition is wrong
    */
  def getExecutionPlan(getSource: String => EitherRP): EitherRP

}
