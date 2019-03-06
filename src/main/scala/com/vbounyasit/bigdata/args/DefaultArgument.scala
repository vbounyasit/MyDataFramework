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

package com.vbounyasit.bigdata.args

import com.vbounyasit.bigdata.args.base.BaseArgument

/**
  * The default arguments to be parsed
  *
  * @param database The output database to store the result(s) in
  * @param table    The output table to store the result(s) in
  * @param env      The environment which to extract the input sources from
  */
case class DefaultArgument(database: String,
                           table: String,
                           env: String) extends BaseArgument[DefaultArgument] {
  override def withDatabaseAndTable(value1: String, value2: String): DefaultArgument =
    copy(database = value1, table = value2)

  override def withEnv(value: String): DefaultArgument = copy(env = value)
}

object DefaultArgument {
  def apply(): DefaultArgument =
    DefaultArgument(
      "N/A",
      "N/A",
      defaultEnv)
}
