/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 8:23 PM
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

import com.vbounyasit.bigdata.args.ArgumentDefinition
import com.vbounyasit.bigdata.args.ArgumentDefinition.ParamValidation
import com.vbounyasit.bigdata.config.ArgumentsConfiguration

class OutputArgumentsConf extends ArgumentsConfiguration[OutputArguments]{

  override val name: String = "Output params"

  override val defaultArguments: OutputArguments = OutputArguments(
    "N/A",
    "N/A",
    "default")

  override val argumentsConfiguration: Map[String, ArgumentDefinition[OutputArguments]] = Map(
    "output_table" -> ArgumentDefinition(
      description = "The output table to write the result to. The argument format should be <database>.<table>",
      argumentMapping = (param, arguments) => {
        val split = param.split('.')
        arguments.copy(database = split(0), table = split(1))
      },
      paramValidation = Some(
        ParamValidation(
          _.matches("^([^\\.])+(\\.)([^\\.])+$"),
          "Database should be in format <database>.<table>"
        )
      )
    ),
    "env" -> ArgumentDefinition(
      description = "The environment where the sources will be taken from",
      argumentMapping = (param, arguments) => arguments.copy(env = param)
    )
  )
}
