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

package com.vbounyasit.bigdata.exceptions

import pureconfig.error.ConfigReaderFailures

abstract class ExceptionHandler extends Exception {
  protected val exceptionType: String
  protected val constructedMessage: String

  override def getMessage: String = s"[ERROR]$exceptionType : $constructedMessage"
}

object ExceptionHandler {

  /**
    * Exceptions related to a specific job.
    */
  abstract class JobError(jobName: String) extends ExceptionHandler {
    override protected val exceptionType: String = s"Job $jobName error"
  }

  case class JobNotFoundError(jobName: String) extends JobError(jobName) {
    override protected val constructedMessage: String = s"The job does not exist."
  }

  case class ExecutionPlanNotFoundError(jobName: String) extends JobError(jobName) {
    override protected val constructedMessage: String = s"Cannot find execution plan."
  }

  case class JobOutputMetadataError(jobName: String, isUpsertError: Boolean, isInsertError: Boolean) extends JobError(jobName) {
    override protected val constructedMessage: String = {
      if (isUpsertError)
        "The output save is in upsert mode, but no merge keys have been specified in the job metadata"
      else if (isInsertError)
        "The output save is in insert mode, please do not specify any merge keys in the job metadata"
      else "The output save mode value must be either 'insert' or 'upsert'"
    }
  }

  /**
    * Exceptions related to a job's input sources.
    */
  abstract class JobSourcesError(jobName: String, sources: Seq[String]) extends ExceptionHandler {
    protected val messageHeader: String
    override protected val exceptionType: String = s"Job $jobName - sources error"
    override protected lazy val constructedMessage: String = s"$messageHeader - ${sources.mkString(",")}."
  }

  case class JobSourcesNotFoundError(jobName: String, sources: String*) extends JobSourcesError(jobName, sources) {
    override protected val messageHeader: String = s"Sources incorrectly or not defined"
  }

  case class JobSourcesDuplicatesFoundError(jobName: String, sources: String*) extends JobSourcesError(jobName, sources) {
    override protected val messageHeader: String = s"Duplicate sources defined"
  }

  /**
    * Other Exceptions
    */
  case class JoinKeyMissingError() extends ExceptionHandler {
    override protected val exceptionType: String = "Join Error"
    override protected val constructedMessage: String = "No join keys were specified."
  }

  case class ParseArgumentsError() extends ExceptionHandler {
    override protected val exceptionType: String = "Argument error"
    override protected val constructedMessage: String = "Cannot parse arguments."
  }

  case class ConfigLoadingError(configName: String, configErrors: ConfigReaderFailures) extends ExceptionHandler {
    override protected val exceptionType: String = s"Config loading error : $configName"
    override protected val constructedMessage: String = configErrors.toList.map(_.description).mkString("\n")
  }

  case class NoOutputTablesSpecified() extends ExceptionHandler {
    override protected val exceptionType: String = "No output tables specified"
    override protected val constructedMessage: String = "Please specify output tables either in argument command line or the ConfigDefinition class"
  }

  case class MergingMapKeyNotFound(key: String) extends ExceptionHandler {
    override protected val exceptionType: String = s"[$key] not found"
    override protected val constructedMessage: String = s"Cannot find [$key] in map for map merging procedure"
  }

  /**
    * Test exceptions
    */
  trait ReadDataFramesFromFilesError extends ExceptionHandler {
    override protected val exceptionType: String = "DataFrame reading error"
  }

  case class CorruptedResourceFile(jobName: String, dataFrameFileName: String) extends ReadDataFramesFromFilesError {
    override protected val constructedMessage: String = s"Corrupted record(s) in $dataFrameFileName. Please check the file content for syntax errors."
  }

  case class ResourcesDataFileNotFound(jobName: String, dataFrameFileName: String) extends ReadDataFramesFromFilesError {
    override protected val constructedMessage: String = s"File $dataFrameFileName not found for $jobName"
  }

  case class CouldNotClearDirectory(folderPath: String) extends ExceptionHandler {
    override protected val exceptionType: String = "IO Error"
    override protected val constructedMessage: String = s"Could not clear directory : $folderPath."
  }

}
