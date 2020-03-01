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

import com.vbounyasit.bigdata.ExceptionWithMessage
import com.vbounyasit.bigdata.exceptions.ErrorHandler.MergingMapKeyNotFound
import cats.implicits._

import scala.reflect.{ClassTag, classTag}

/**
  * Utilities for operation on collections
  */
object CollectionsUtils {

  def partition[V, T <: V : ClassTag, U <: V : ClassTag](listParent: List[V]): (List[T], List[U]) = {
    val emptyList: (List[T], List[U]) = (Nil, Nil)
    listParent.foldRight(emptyList) {
      case (parent, (children1, children2)) => parent match {
        case child1: T if classTag[T].runtimeClass.isInstance(child1) => (child1 :: children1, children2)
        case child2: U if classTag[U].runtimeClass.isInstance(child2) => (children1, child2 :: children2)
      }
    }
  }

  /**
    * Will merge two maps by tupling the values together. Both maps must both contain the exact same keys
    *
    * @param toMerge               The first map
    * @param mergeWith               The second map
    * @param errorOnKeyMatching A String => ErrorHandler variable that will be used as Exception in case of keys matching error
    * @return A merged map of String -> tuple
    */
  def mergeByKeyStrict[U, V](toMerge: Map[String, U], mergeWith: Map[String, V], errorOnKeyMatching: ExceptionWithMessage[MergingMapKeyNotFound]): Either[MergingMapKeyNotFound, Map[String, (U, V)]] = {
    toMerge
      .map {
        case (key1, value1) => mergeWith.get(key1) match {
          case Some(value2) => Right((key1, (value1, value2)))
          case None => Left(errorOnKeyMatching(key1))
        }
      }
      .toList
      .sequence
      .map(_.toMap)
  }
}
