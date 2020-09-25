/*
 * Developed by Vibert Bounyasit
 * Last modified 9/18/19 8:30 PM
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

import com.vbounyasit.bigdata.args.ArgumentDefinition.ParamValidation

/**
  * A definition for each arguments provided in the command line while submitting the Spark job
  *
  * @param argConstructor A function of that will construct the argument case class that will be used
  *                        in your job.
  *                        NB: (arg, Arguments object) => Arguments object
  * @param description     The description of the argument parameter
  * @param paramValidation A function that validates the format of the argument provided
  * @tparam T The Arguments object
  */
case class ArgumentDefinition[T](description: String,
                                 argConstructor: (String, T) => T,
                                 paramValidation: Option[ParamValidation] = None)

object ArgumentDefinition {

  /**
    * Validates a command line parameter
    *
    * @param validate       The boolean function for validation
    * @param failureMessage The failure message in case of non valid parameter
    */
  case class ParamValidation(validate: String => Boolean, failureMessage: String)

}