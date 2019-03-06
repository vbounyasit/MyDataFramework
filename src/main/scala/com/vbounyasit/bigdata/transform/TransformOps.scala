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

package com.vbounyasit.bigdata.transform

import cats.implicits._
import com.vbounyasit.bigdata.EitherRP
import com.vbounyasit.bigdata.transform.joiner.Joiner
import com.vbounyasit.bigdata.transform.pipeline.impl.{Pipeline, SourcePipeline}
import org.apache.spark.sql.DataFrame

/**
  * A Type class for defining operations related to source pipelines
  *
  * @tparam U The type we want to define such operations on
  */
trait TransformOps[U] {
  def join(toJoin: U, joinWith: U, joiner: Joiner): EitherRP

  def union(toUnion: U, unionWith: U): U
}

/**
  * [Important : import these functions in any class we want to perform data processing operations)
  * An object containing all the implicits needed for performing transformations in the execution plan
  */
object TransformOps {

  //todo maybe turn the whole transformation theory into Monoids (cats)

  implicit class PipelineOperator[U](value: U) {
    def ==>[V, W](target: V)(implicit appender: (U, V) => W): W = {
      appender(value, target)
    }
  }

  implicit def transformersToPipeline: Seq[Transformer] => Pipeline = Pipeline(_: _*)

  implicit def transformerToPipeline: Transformer => Pipeline = Seq(_)

  implicit def functionToPipeline: (DataFrame => DataFrame) => Pipeline = f => new Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = f(dataFrame)
  }

  implicit def ToPipelineAppender[U, V, W](implicit
                                           appender: (U, Pipeline) => W,
                                           converter: V => Pipeline): (U, V) => W =
    (element, target) => appender(element, converter(target))

  /**
    * Appenders
    */

  implicit def withPipelineAppender[T](implicit converter: T => Pipeline, appender: (Pipeline, Pipeline) => Pipeline): (T, Pipeline) => Pipeline =
    (element, target) => appender(converter(element), target)

  implicit val eitherPipelineWithPipelineAppender: (EitherRP, Pipeline) => EitherRP =
    (element, target) => element.right.map(_ ==> target)

  implicit val sPipelineWithPipelineAppender: (SourcePipeline, Pipeline) => SourcePipeline =
    (element, target) => element.copy(tail = element.tail.transformers ++ target.transformers)

  implicit val pipelineWithPipelineAppender: (Pipeline, Pipeline) => Pipeline =
    (element, target) => Pipeline(element.transformers ++ target.transformers: _*)

  implicit val dataFrameWithPipelineAppender: (DataFrame, Pipeline) => SourcePipeline =
    (element, target) => SourcePipeline(element, target.transformers)

  /**
    * Operations
    */

  implicit def sPipelineOperations: TransformOps[SourcePipeline] = new TransformOps[SourcePipeline] {
    override def join(toJoin: SourcePipeline, joinWith: SourcePipeline, joiner: Joiner): EitherRP =
      toJoin.join(joinWith, joiner)

    override def union(toUnion: SourcePipeline, unionWith: SourcePipeline): SourcePipeline =
      toUnion.union(unionWith)
  }

  implicit def eitherSPipelineOperations: TransformOps[EitherRP] = new TransformOps[EitherRP] {
    override def join(toJoin: EitherRP, joinWith: EitherRP, joiner: Joiner): EitherRP =
      for {
        lPipeline <- toJoin
        rPipeline <- joinWith
        joined <- lPipeline.join(rPipeline, joiner)
      } yield joined


    override def union(toUnion: EitherRP, unionWith: EitherRP): EitherRP =
      for {
        pipeline1 <- toUnion
        pipeline2 <- unionWith
      } yield pipeline1 union pipeline2
  }

}



