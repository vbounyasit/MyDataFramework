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
import com.vbounyasit.bigdata.args.ArgumentsConfiguration
import com.vbounyasit.bigdata.config.OutputTablesGenerator.ResultTables
import com.vbounyasit.bigdata.{ApplicationConf, OutputTables}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

/**
  * Everything related to configuration files loading is handled here.
  */
trait ConfigDefinition {

  type SparkDependentConfig = SparkSession => Config

  implicit def function1ToOutputTablesInfo(function1: _ => OutputTables): ResultTables = OutputTablesGenerator(function1)

  implicit def function2ToOutputTablesInfo(function2: (_, _) => OutputTables): ResultTables = OutputTablesGenerator(function2)

  implicit def toSparkDependentConfig(config: Config): SparkDependentConfig = _ => config

  def loadConfig[T](configName: String, config: Config)(implicit reader: ConfigReader[T]): ApplicationConf[T] = {
    Some(ConfigurationsLoader.loadConfig[T](configName, config))
  }

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
  val sourcesConf: SparkDependentConfig

  /**
    * The configuration for the different jobs we have.
    */
  val jobsConf: SparkDependentConfig

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
  val outputTables: ResultTables


}
