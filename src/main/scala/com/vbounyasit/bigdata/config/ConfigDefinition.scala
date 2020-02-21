/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 6:56 PM
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

import com.typesafe.config.{Config, ConfigFactory}
import com.vbounyasit.bigdata.ApplicationConf
import com.vbounyasit.bigdata.ETL.TableMetadata
import com.vbounyasit.bigdata.args.ArgumentsConfiguration

/**
  * Everything related to configuration files loading is handled here.
  */
trait ConfigDefinition {

  implicit def toOptionalOutputTables(tables: Seq[(String, String)]): Option[Seq[(String, String)]] = Some(tables)

  /**
    * The spark parameters that will be used on the remote cluster we submit our job on.
    */
  val sparkConf: Config = ConfigFactory.load("spark_params")

  /**
    * The spark parameters that will be used when running Unit/Integration tests on our local machine.
    */
  val sparkLocalConf: Config = ConfigFactory.load("spark_params_local")

  /**
    * The configuration related to the different input sources we can use.
    */
  val sourcesConf: Config

  /**
    * The configuration for the different jobs we have.
    */
  val jobsConf: Config

  /**
    * An optional configuration file related to our application.
    *
    * Note: On the Application side, you can fill this parameter using the
    * loadConfig function from pureconfig
    */
  val applicationConf: ApplicationConf[_] = None

  /**
    * The arguments parameters that will be parsed for every jobs launched
    */
  val applicationArguments: Option[ArgumentsConfiguration[_]] = None

  /**
    * The output tables and jobs to run
    */
  def resultingOutputTables[GlobalConfig, GlobalArgument](applicationConf: Option[GlobalConfig],
                                                          applicationArgument: GlobalArgument): Option[Seq[TableMetadata]] = None


}
