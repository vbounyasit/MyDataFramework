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

package com.vbounyasit.bigdata.sample

import com.typesafe.config.{Config, ConfigFactory}
import com.vbounyasit.bigdata.ApplicationConf
import com.vbounyasit.bigdata.SparkApplication.ApplicationConfData
import com.vbounyasit.bigdata.config.ConfigDefinition
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.sample.data.SampleApplicationConf
import pureconfig.generic.auto._

class SampleConfigDefinition extends ConfigDefinition {

  override val applicationConf: ApplicationConf[SampleApplicationConf] =
    ApplicationConfData(
      "Application conf",
      pureconfig.loadConfig[SampleApplicationConf](ConfigFactory.load())
    )

  override val sourcesConf: Config = ConfigFactory.load("sources")

  override val jobsConf: Config = ConfigFactory.load("jobs_params")


}
