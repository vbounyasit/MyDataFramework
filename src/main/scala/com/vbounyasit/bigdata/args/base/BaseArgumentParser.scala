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

package com.vbounyasit.bigdata.args.base

import com.vbounyasit.bigdata.args.ArgumentsDefinition
import scopt.OParser

class BaseArgumentParser[T <: BaseArgument[T]] extends ArgumentsDefinition[T]("Default arguments") {

  import builder._

  /**
    * The arguments parsing logic
    */
  override protected val arguments: OParser[_, T] =
    OParser.sequence(
      (opt[String]("output_table") required() action (
        (x, c) => {
          val split = x.split('.')
          c.withDatabaseAndTable(database = split(0), table = split(1))
        }) text "The output table to write the result to. The argument format should be <database>.<table>")
        .validate(x => {
          val argFormat = "^([^\\.])+(\\.)([^\\.])+$"
          if (x.matches(argFormat)) success else failure("Database should be in format <database>.<table>")
        }),
      opt[String]("env")
        .action((x, c) => c.withEnv(x))
        .text("The environment where the sources will be taken from")
    )
}
