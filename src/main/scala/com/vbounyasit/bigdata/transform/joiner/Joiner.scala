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

package com.vbounyasit.bigdata.transform.joiner

import com.vbounyasit.bigdata.transform.joiner.JoinerKeys.JoinKey
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Trait defining a join operation between two dataFrames/Pipelines
  */
trait Joiner {

  /**
    * The join keys definition.
    */
  val keys: List[JoinKey]

  /**
    * The join type (Note : 'left' by default against 'inner' on spark default join function).
    */
  val joinType: String = "left"

  /**
    * Additional predicates to add to the join operation.
    */
  val joinPredicate: Option[Column] = None

  /**
    * An optional transformer that will be applied right after the join operation.
    */
  val postJoinTransformer: Option[DataFrame => DataFrame] = None

}
