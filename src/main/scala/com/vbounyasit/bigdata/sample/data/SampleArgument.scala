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

package com.vbounyasit.bigdata.sample.data

import com.vbounyasit.bigdata.args.timefilter.TimeFilterArgument
import com.vbounyasit.bigdata.sample.datePattern
import com.vbounyasit.bigdata.utils.DateUtils

case class SampleArgument(startDate: String, endDate: String) extends TimeFilterArgument[SampleArgument] {
  override def withStartDate(value: String): SampleArgument = copy(startDate = value)

  override def withEndDate(value: String): SampleArgument = copy(endDate = value)
}

object SampleArgument {
  def apply(): SampleArgument =
    SampleArgument(DateUtils.today(datePattern.pattern), DateUtils.today(datePattern.pattern))
}