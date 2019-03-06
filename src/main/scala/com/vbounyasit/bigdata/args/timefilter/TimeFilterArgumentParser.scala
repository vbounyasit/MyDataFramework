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

package com.vbounyasit.bigdata.args.timefilter

import com.vbounyasit.bigdata.DatePattern
import com.vbounyasit.bigdata.args.ArgumentsDefinition
import com.vbounyasit.bigdata.utils.DateUtils
import scopt.OParser

class TimeFilterArgumentParser[T <: TimeFilterArgument[T]](datePattern: DatePattern) extends ArgumentsDefinition[T]("Time parameters") {

  import builder._

  /**
    * The arguments parsing logic
    */
  override protected val arguments: OParser[_, T] =
    OParser.sequence(
      (opt[String]("start_date") action (
        (x, c) => c.withStartDate(x)
        ) text "A starting date that can be used to filter data on extraction")
        .validate(x =>
          if (DateUtils.isValidDate(x, datePattern.pattern)) success
          else failure(s"The provided argument must be in format ${datePattern.pattern}")
        ),
      (opt[String]("end_date") action (
        (x, c) => c.withEndDate(x)
        ) text "An ending date that can be used to filter data on extraction")
        .validate(x =>
          if (DateUtils.isValidDate(x, datePattern.pattern)) success
          else failure(s"The provided argument must be in format ${datePattern.pattern}")
        )
    )
}
