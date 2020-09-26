/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 8:20 PM
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

import com.vbounyasit.bigdata.exceptions.ErrorHandler.ParseArgumentsError
import com.vbounyasit.bigdata.utils.MonadUtils
import scopt.{OParser, OParserBuilder}

/**
  * A definition used to parse input arguments while running an application
  *
  * @param name The argument definition name
  * @tparam T The case class data type containing the loaded arguments
  */
class ArgumentsParser[T](name: String,
                         defaultArguments: T,
                         argumentsConfiguration: Map[String, ArgumentDefinition[T]]) {

  /**
    * The parser builder
    */
  protected val builder: OParserBuilder[T] = OParser.builder[T]

  import builder._

  /**
    * Builds the argument parsing sequence based on the provided configuration with OParser
    * @param argsConfig A map with the correct argument definitions
    * @return an OParser object that has all the parsing configurations setup
    */
  def buildArguments(argsConfig: Map[String, ArgumentDefinition[T]]): OParser[_, T] = {
    val parserSequence = argsConfig.map {
      case (key, value) =>
        var parsing: OParser[String, T] =
          opt[String](name = key) action value.argConstructor text value.description
        if(value.isRequired) parsing = parsing required

        if (value.paramValidation.isEmpty)
          parsing
        else
          parsing.validate(param => {
            val validator = value.paramValidation.get
            if (validator.validate(param))
              success
            else
              failure(validator.failureMessage)
          })
    }
    builder.note(s"\n$name\n") ++ OParser.sequence(
      parserSequence.head,
      parserSequence.tail.toSeq: _*
    )
  }

  /**
    * Builds the final parser adding the app name and other meta info
    * @param appName The application name
    * @param argsConfig The argument definition map
    * @return The result parser
    */
  private def buildParser(appName: String,
                          argsConfig: Map[String, ArgumentDefinition[T]]): OParser[_, T] = {
    OParser.sequence(
      programName(appName),
      head("scopt", "4.x"),
      buildArguments(argsConfig)
    )
  }

  /**
    * Parses A list of arguments provided through the command line in the 'main' method
    * @param appName The application name
    * @param args The args parameter provided by the main function
    * @return Either the resulting Arguments object or an error
    */
  def parseArguments(appName: String,
                     args: Array[String]): Either[ParseArgumentsError, T] = {

    val argumentOption: Option[T] = OParser.parse(
      buildParser(appName, argumentsConfiguration),
      args,
      defaultArguments,
      new ParserSetup()
    )
    MonadUtils.optionToEither(argumentOption, ParseArgumentsError())
  }
}
