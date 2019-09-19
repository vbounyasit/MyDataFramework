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

package com.vbounyasit.bigdata.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.{Failure, Success, Try}

/**
  * Utilities for date related operations
  */
object DateUtils {

  def today(pattern: String): String = {
    Try({
      val formatter = DateTimeFormatter.ofPattern(pattern)
      LocalDate.now().format(formatter)
    }) match {
      case Success(date) => date
      case Failure(exception) => throw exception
    }
  }

  def isValidDate(date: String, pattern: String): Boolean = {
    Try({
      val formatter = DateTimeFormatter.ofPattern(pattern)
      formatter.parse(date)
    }).isSuccess
  }
}
