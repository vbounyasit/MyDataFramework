/*
 * Developed by Vibert Bounyasit
 * Last modified 6/13/19 4:35 AM
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

package com.vbounyasit.bigdata.transform.pipeline.impl

import com.vbounyasit.bigdata.exceptions.ExceptionHandler.JoinKeyMissingError
import com.vbounyasit.bigdata.implicits._
import com.vbounyasit.bigdata.transform.TransformOps._
import com.vbounyasit.bigdata.transform.joiner.Joiner
import com.vbounyasit.bigdata.transform.joiner.JoinerKeys.{CommonKey, JoinKey, KeyProjection}
import com.vbounyasit.bigdata.utils.CollectionsUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * A DataFrame with a set of transformations in a Pipeline object.
  */
case class SourcePipeline(head: DataFrame, tail: Pipeline = Pipeline()) {

  /**
    * Apply the pipeline transformations to the dataFrame.
    *
    * @return The resulting DataFrame
    */
  def transform: DataFrame =
    tail.transformers.foldLeft(head: DataFrame)(
      (dataFrame, transformer) => transformer.transform(dataFrame)
    )

  /**
    * Applies a join transformation with another SourcePipelines.
    *
    * @param pipeline The pipeline we want to join with
    * @param joiner   The join operation definition
    * @return Either the new SourcePipeline or a JoinKeyMissingError
    */
  def join(pipeline: SourcePipeline,
           joiner: Joiner): Either[JoinKeyMissingError, SourcePipeline] = {
    joiner.keys match {
      case Nil => Left(JoinKeyMissingError())
      case keys =>
        val (commonKeys, keyProjections) = CollectionsUtils.partition[JoinKey, CommonKey, KeyProjection](keys)
        Right(
          join(pipeline)(commonKeys, keyProjections, joinExpr = joiner.joinPredicate, jType = joiner.joinType, postTransformation = joiner.postJoinTransformer)
        )
    }
  }

  /**
    * Performs an union operation with another pipeline
    *
    * @param pipeline The pipeline we want to unite with
    * @return Tbe resulting SourcePipeline
    */
  def union(pipeline: SourcePipeline): SourcePipeline = transform union pipeline.transform

  /**
    * Perform a join operation with given parameters
    *
    * @param pipeline           The pipeline we want to join with
    * @param commonKeys         Common join keys between our dataFrames
    * @param keyProjections     Key projections for different join keys between the dataFrames
    * @param joinExpr           An optional predicate to include in the join conditions
    * @param jType              The join type
    * @param postTransformation An optional transformation to apply after the join operation
    * @return The resulting SourcePipeline
    */
  private def join(pipeline: SourcePipeline)
                  (commonKeys: List[CommonKey] = List(),
                   keyProjections: List[KeyProjection] = List(),
                   joinExpr: Option[Column] = None,
                   jType: String = "inner",
                   postTransformation: Option[DataFrame => DataFrame]): SourcePipeline = {

    val leftPrefix = "l_"
    val rightPrefix = "r_"

    def leftDF: DataFrame =
      renameColumns(
        dataFrame = transform,
        renamingConf = commonKeys.map(_.key).map(key => (key, leftPrefix + key))
      )

    def rightDF: DataFrame =
      renameColumns(
        dataFrame = pipeline.transform,
        renamingConf = commonKeys.map(_.key).map(key => (key, rightPrefix + key))
      )

    def joinKeys: Seq[(String, String, String)] =
      commonKeys.map(_.key).map(key => (leftPrefix + key, rightPrefix + key, key)) ++ keyProjections.map(key => KeyProjection.unapply(key).get)

    def initialJoinExpr: Column = joinExpr match {
      case None => lit(true)
      case Some(expr) => expr
    }

    val postJoinKeyTransform: DataFrame => DataFrame = {
      dataFrame => {
        joinKeys.foldLeft(dataFrame) {
          case (acc, (leftKey, rightKey, resultKey)) =>
            acc.withColumnRenamed(leftKey, resultKey).drop(rightKey)
        }
      }
    }
    val identityTransformation: DataFrame => DataFrame = df => df
    SourcePipeline(
      postTransformation.getOrElse(identityTransformation)(
        leftDF.join(rightDF,
          joinKeys.foldLeft(initialJoinExpr) {
            case (acc, (leftKey, rightKey, _)) =>
              acc and col(leftKey) === col(rightKey)
          },
          jType
        )
      )
    ) ==> postJoinKeyTransform
  }

  /**
    * An utility to rename columns in a dataFrame
    *
    * @param dataFrame    The input dataFrame
    * @param renamingConf The renaming configuration
    * @return The resulting dataFrame
    */
  private def renameColumns(dataFrame: DataFrame, renamingConf: Seq[(String, String)]): DataFrame = {
    renamingConf.foldLeft(dataFrame) {
      case (acc, (toRename, renameTo)) =>
        acc.withColumnRenamed(toRename, renameTo)
    }
  }

}
