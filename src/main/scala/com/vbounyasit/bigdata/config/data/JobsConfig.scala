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

package com.vbounyasit.bigdata.config.data

object JobsConfig {

  case class JobsConf(jobs: Map[String, JobConf])

  case class JobConf(outputMetadata: OutputMetaData, sources: List[JobSource])

  case class OutputMetaData(testIdColumn: String = "", dateColumn: String = "", outputColumns: Seq[String] = Seq())

  case class JobSource(sourceName: String, selectedColumns: List[String], dropDuplicates: Boolean = true)

}
