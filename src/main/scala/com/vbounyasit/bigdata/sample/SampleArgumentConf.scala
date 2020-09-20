/*
 * Developed by Vibert Bounyasit
 * Last modified 9/19/19 2:25 PM
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

import com.vbounyasit.bigdata.args.{ArgumentDefinition, ArgumentsConfiguration}
import com.vbounyasit.bigdata.args.ArgumentDefinition.ParamValidation
import com.vbounyasit.bigdata.sample.data.SampleArgs
import com.vbounyasit.bigdata.utils.DateUtils

class SampleArgumentConf extends ArgumentsConfiguration[SampleArgs]{

  override val name: String = "Sample arguments"

  override val defaultArguments: SampleArgs = SampleArgs(DateUtils.today(datePattern.pattern), DateUtils.today(datePattern.pattern))

  override val argumentsConfiguration: Map[String, ArgumentDefinition[SampleArgs]] = Map(
    "start-date" -> ArgumentDefinition(
      description = "The start date to filter data by",
      argumentMapping = (param, arg) => arg.copy(startDate = param),
      paramValidation = Some(ParamValidation(param => DateUtils.isValidDate(param, datePattern.pattern), s"The provided argument must be in format ${datePattern.pattern}"))
    ),
    "end-date" -> ArgumentDefinition(
      description = "The end date to filter data by",
      argumentMapping = (param, arg) => arg.copy(endDate = param),
      paramValidation = Some(ParamValidation(param => DateUtils.isValidDate(param, datePattern.pattern), s"The provided argument must be in format ${datePattern.pattern}"))
    )
  )
}
