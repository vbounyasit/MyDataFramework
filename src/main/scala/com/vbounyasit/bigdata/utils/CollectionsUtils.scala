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

package com.vbounyasit.bigdata.utils

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
}
