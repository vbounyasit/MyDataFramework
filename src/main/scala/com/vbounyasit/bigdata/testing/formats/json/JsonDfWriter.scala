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

package com.vbounyasit.bigdata.testing.formats.json

import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameWriter
import org.apache.spark.sql.DataFrame

class JsonDfWriter extends DataFrameWriter("json") {

  /**
    * The string obtained from a given DataFrame to write to resources
    *
    * @param dataFrame the DataFrame to convert
    * @return The converted String
    */
  override def dataFrameStringConversion(dataFrame: DataFrame): String = {

    s"[${toJsonWithFilledNulls(dataFrame).mkString(",")}]"
      .replaceAll(",", ",\n  ")
      .replaceAll(":", ": ")
      .replace("},\n  ", "},")
      .replace("}", "\n}")
      .replace("{", "{\n  ")
  }

  private def toJsonWithFilledNulls(dataFrame: DataFrame): Array[String] = {
    val schema: Seq[String] = dataFrame.schema.map(_.name)
    dataFrame.toJSON.collect().map(chain => {
      val elements: Map[String, String] = chain
        .replaceAll("[{}]", "")
        .split(",")
        .map(e => e.split("\"")(1) -> e).toMap

      val nullFilledJson = schema.map(column => {
        elements.get(column) match {
          case Some(jsonValue) => jsonValue
          case None => "\"" + column + "\":null"
        }
      }).mkString(",")
      s"{$nullFilledJson}"
    })
  }
}
