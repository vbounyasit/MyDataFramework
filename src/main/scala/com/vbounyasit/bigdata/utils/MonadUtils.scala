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

import cats.implicits._
import com.vbounyasit.bigdata.ExceptionWithMessage
import com.vbounyasit.bigdata.exceptions.ErrorHandler

/**
  * Utilities related to Monad operations
  */
object MonadUtils {

  /**
    * Automatically handle either exceptions
    */
  def handleEither[T](either: Either[ErrorHandler, T]): T = either match {
    case Right(result) => result
    case Left(error) => throw error
  }

  def optionToEither[T, V <: ErrorHandler](option: Option[T], left: V): Either[V, T] = {
    Either.cond(option.isDefined, option.get, left)
  }

  def getMapSubList[K, V, Error <: ErrorHandler](keySubSeq: List[K], dataMap: Map[K, V], errorNotFound: ExceptionWithMessage[Error])(implicit keyConverter: K => String): Either[Error, Map[K, V]] = {
    def toEither[U](key: K, option: Option[(K, U)]): Either[Error, (K, U)] = Either.cond(option.isDefined, option.get, errorNotFound(keyConverter(key)))
    keySubSeq
      .map(key => (key, dataMap.get(key).map(key -> _)))
      .map(e => toEither(e._1, e._2))
      .sequence.map(_.toMap)
  }
}
