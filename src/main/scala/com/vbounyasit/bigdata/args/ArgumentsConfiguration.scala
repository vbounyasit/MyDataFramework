/*
 * Developed by Vibert Bounyasit
 * Last modified 9/19/19 3:12 PM
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

package com.vbounyasit.bigdata.args

/**
  * A trait that contains all the defined configurations for command line argument parsing
 *
  * @tparam T The type of the Arguments object we will use in our processing plan
  */
trait ArgumentsConfiguration[T] {

  /**
    * The name of the set of arguments
    */
  val name: String

  /**
    * The default argument (if none or not all of the arguments needed are provided)
    */
  val defaultArguments: T

  /**
    * Mapping of the list of the arguments to provide
    */
  val argumentsConfiguration: Map[String, ArgumentDefinition[T]]

  /**
    * The parser object
    */
  lazy final val argumentParser = new ArgumentsParser[T](name, defaultArguments, argumentsConfiguration)
}
