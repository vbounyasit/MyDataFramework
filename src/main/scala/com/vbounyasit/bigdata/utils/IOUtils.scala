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

import java.io.File

import com.vbounyasit.bigdata.exceptions.ErrorHandler.CouldNotClearDirectory
import org.apache.commons.io.FileUtils

import scala.util.Try

/**
  * Utilities for I/O file operations
  */
object IOUtils {
  /**
    * Write content to file in Resources folder
    *
    * @param content the file content
    * @param path    the file path
    */
  def writeToResources(content: String, path: String): Unit = {
    FileUtils.writeStringToFile(new File(s"$resourcesPath/$path"), content)
  }

  def readFromResources(path: String): Option[String] = {
    Try(FileUtils.readFileToString(new File(s"$resourcesPath/$path"))).toOption
  }

  /**
    * Deleting files
    *
    * @param files the files to delete
    */
  def deleteFiles(files: Seq[File], path: String): Unit = {
    files.foreach(file =>
      if (!file.delete()) throw CouldNotClearDirectory(file.getPath)
    )
  }

  /**
    * Returns the path to the resources package
    */
  val resourcesPath: String = {
    val basePath = this.getClass.getResource("/sources.conf").getPath.split("/").dropRight(4).mkString("/")
    val resourcePath = s"$basePath/src/test/resources"
    resourcePath
  }
}
