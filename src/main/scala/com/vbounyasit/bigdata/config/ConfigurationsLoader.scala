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

package com.vbounyasit.bigdata.config

import cats.implicits._
import com.typesafe.config.Config
import com.vbounyasit.bigdata.config.data.JobsConfig.JobsConf
import com.vbounyasit.bigdata.config.data.SourcesConfig.SourcesConf
import com.vbounyasit.bigdata.config.data.SparkParamsConf
import com.vbounyasit.bigdata.exceptions.ExceptionHandler
import com.vbounyasit.bigdata.exceptions.ExceptionHandler.ConfigLoadingError
import pureconfig.ConfigReader
import pureconfig.error.ConfigReaderFailures

/**
  * A case class representing the different configurations we want to load for our application.
  *
  * @param sparkParamsConf The Spark parameters used while running the application
  * @param sourcesConf     The configuration for the pool of sources we want to extract data from
  * @param jobsConf        The configuration related to our processing jobs
  */
case class ConfigurationsLoader(sparkParamsConf: SparkParamsConf,
                                sourcesConf: SourcesConf,
                                jobsConf: JobsConf)

object ConfigurationsLoader {

  /**
    * Loads a set of configuration data through a provided ConfigDefinition
    *
    * @param configDefinition    The provided ConfigDefinition
    * @param useLocalSparkParams Whether or not to use the local params for spark
    * @return Either the ConfigurationsLoader object containing the loaded data or an Exception
    */
  def apply(configDefinition: ConfigDefinition, useLocalSparkParams: Boolean = false): Either[ExceptionHandler, ConfigurationsLoader] = {
    import pureconfig.generic.auto._
    for {
      sparkParamsConf <- {
        val conf = if (useLocalSparkParams) configDefinition.sparkLocalConf else configDefinition.sparkConf
        loadConfig[SparkParamsConf]("Spark conf", conf)
      }
      sourcesConf <- loadConfig[SourcesConf]("Sources conf", configDefinition.sourcesConf)
      jobsConf <- loadConfig[JobsConf]("Jobs conf", configDefinition.jobsConf)
    } yield {
      ConfigurationsLoader(sparkParamsConf, sourcesConf, jobsConf)
    }
  }

  /**
    * Loads a configuration via pureconfig from a Config object
    *
    * @param configName The name of the configuration
    * @param config     The Config object loaded from a .conf file
    * @param reader     The implicit config reader (pureconfig)
    * @tparam T The Configuration case class type
    * @return Either the Configuration data or a configuration loading Exception
    */
  def loadConfig[T](configName: String, config: Config)(implicit reader: ConfigReader[T]): Either[ExceptionHandler, T] = {
    pureconfig
      .loadConfig[T](config)
      .left
      .map(error => ConfigLoadingError(configName, error))
  }

  /**
    * Map the ConfigReaderFailures into a ConfigLoadingError
    * from an already loaded configuration via pureconfig
    *
    * @param configName The name of the configuration
    * @param config     The Config object loaded from a .conf file
    * @tparam T The Configuration case class type
    * @return Either the Configuration data or a configuration loading Exception
    */
  def loadConfig[T](configName: String, config: Either[ConfigReaderFailures, _]): Either[ExceptionHandler, _] = {
    config
      .left
      .map(error => ConfigLoadingError(configName, error))
  }

}
