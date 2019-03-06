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

package com.vbounyasit.bigdata.testing

import com.vbounyasit.bigdata.testing.data.UnitTestDefinition.UnitTest
import com.vbounyasit.bigdata.transform.TransformComponents.ExecutionPipelines

/**
  * A trait representing the definition of unit tests
  */
trait UnitTestDefinition {

  /**
    * The pipeline definition class of our application
    */
  protected val executionPipelines: ExecutionPipelines

  /**
    * A sequence of unit tests we want to run
    */
  val unitTests: Seq[UnitTest]

}
