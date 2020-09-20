/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 3:54 AM
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

import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.testing.JobsTestGenerator
import com.vbounyasit.bigdata.testing.common.HiveEnvironment
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameWriter
import com.vbounyasit.bigdata.testing.formats.json.{JsonDfLoader, JsonDfWriter}

class SampleTestGenerator extends JobsTestGenerator {

  override val sparkApplication: SparkApplication[_, _] = SampleApplication

  override val hiveEnvironment: HiveEnvironment = new HiveEnvironment(new JsonDfLoader)

  override val dataFrameWriter: DataFrameWriter = new JsonDfWriter

  executeTests()
}
