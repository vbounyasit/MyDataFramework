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

import com.vbounyasit.bigdata.exceptions.ExceptionHandler
import cats.implicits._
import com.vbounyasit.bigdata.ExceptionWithMessage

/**
  * Utilities related to Monad operations
  */
object MonadUtils {

  def optionToEither[T, V <: ExceptionHandler](option: Option[T], left: V): Either[V, T] = {
    Either.cond(option.isDefined, option.get, left)
  }

  def getMapSubList[K, V, Error <: ExceptionHandler](keySubSeq: List[K], dataMap: Map[K, V], errorNotFound: ExceptionWithMessage[Error])(implicit keyConverter: K => String): Either[Error, Map[K, V]] = {
    def toEither[U](key: K, option: Option[(K, U)]): Either[Error, (K, U)] = Either.cond(option.isDefined, option.get, errorNotFound(keyConverter(key)))
    keySubSeq
      .map(key => (key, dataMap.get(key).map(key -> _)))
      .map(e => toEither(e._1, e._2))
      .sequence.map(_.toMap)
  }
}
