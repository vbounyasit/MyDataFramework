/*
 * Developed by Vibert Bounyasit
 * Last modified 6/11/19 7:59 PM
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

package com.vbounyasit.bigdata.sample.joiner

import com.vbounyasit.bigdata.transform.joiner.Joiner
import com.vbounyasit.bigdata.transform.joiner.JoinerKeys.{CommonKey, JoinKey}

class SampleJoiner extends Joiner {
  override val keys: List[JoinKey] = List(
    CommonKey("index_col")
  )
  override val joinType: String = "inner"

}
