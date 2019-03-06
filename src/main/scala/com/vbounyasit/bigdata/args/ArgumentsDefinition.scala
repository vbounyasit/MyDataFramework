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

package com.vbounyasit.bigdata.args

import scopt.{OParser, OParserBuilder}

/**
  * A definition used to parse input arguments while running an application
  *
  * @param name The argument definition name
  * @tparam T The case class data type containing the loaded arguments
  */
abstract class ArgumentsDefinition[T](name: String) {

  /**
    * The parser builder
    */
  protected val builder: OParserBuilder[T] = OParser.builder[T]

  /**
    * The arguments parsing logic
    */
  protected val arguments: OParser[_, T]

  /**
    * @return The arguments preceded by the arguments definition name
    */
  def getArgumentSequence: OParser[_, T] = {
    builder.note(s"\n$name\n") ++ arguments
  }
}

object ArgumentsDefinition {
  def buildParser[T](appName: String, parsers: ArgumentsDefinition[T]*): OParser[_, T] = {
    val builder = OParser.builder[T]
    import builder._
    OParser.sequence(
      programName(appName),
      head("scopt", "4.x")
        +: parsers.map(_.getArgumentSequence): _*
    )
  }
}
