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

package com.vbounyasit.bigdata

import com.vbounyasit.bigdata.exceptions.ExceptionHandler
import com.vbounyasit.bigdata.providers.LoggerProvider

/**
  * Implicits used in runnable applications
  */
object appImplicits {

  /**
    * Automatically handle either exceptions
    */
  def handleEither[T](either: Either[ExceptionHandler, T]): T = either match {
    case Right(result) => result
    case Left(error) => throw error
  }

  /**
    * Allow us to handle Either objects while logging in between the operations
    */
  implicit class LogEither[T](either: Either[ExceptionHandler, T]) extends LoggerProvider {

    def info(info: String): T = log(info, logger.info)

    def warn(info: String): T = log(info, logger.warn)

    def error(info: String): T = log(info, logger.error)

    def debug(info: String): T = log(info, logger.debug)

    def trace(info: String): T = log(info, logger.trace)

    private def log(info: String, logType: String => Unit): T = {
      either match {
        case Right(x) =>
          logType(info)
          x
        case Left(error) => throw error
      }
    }
  }
}
