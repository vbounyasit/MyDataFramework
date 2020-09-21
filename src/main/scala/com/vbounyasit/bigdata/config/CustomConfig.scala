package com.vbounyasit.bigdata.config

import com.typesafe.config.Config
import com.vbounyasit.bigdata.exceptions.ErrorHandler
import pureconfig.ConfigReader

class CustomConfig[T](val configName: String, val config: Config)(implicit reader: ConfigReader[T]) {
  val loadedConfig: Either[ErrorHandler, T] = ConfigurationsLoader.loadConfig[T](configName, config)
}
