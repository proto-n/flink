/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.ml.evaluation

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.ml.pipeline._

//TODO: Need to generalize type of Score (and evaluateOperation)
abstract class Scorer[
  PredictorType[PredictorInstanceType],
  Testing,
  Prediction
] (val score: AbstractScore[PredictorType, Testing, Prediction])
  extends WithParameters {
  def evaluate[InputTesting, PredictorInstance <: PredictorType[PredictorInstance]](
      testing: DataSet[InputTesting],
      predictorInstance: PredictorInstance,
      evaluateParameters: ParameterMap = ParameterMap.Empty)(implicit
    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, Testing]):
    DataSet[Double]
}

class PairwiseScorer(override val score: PairwiseScore[Double])
  extends Scorer(score) with WithParameters
{

  override def evaluate[InputTesting, PredictorInstance <: Predictor[PredictorInstance]](
    testing: DataSet[InputTesting],
    predictorInstance: PredictorInstance,
    evaluateParameters: ParameterMap)(implicit
    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, (Double, Double)])
  : DataSet[Double] = {
    val resultingParameters = predictorInstance.parameters ++ evaluateParameters
    val predictions = prepareOperation.prepare(predictorInstance, testing, resultingParameters)
    score.evaluate(predictions)
  }
}

class RankingScorer(override val score: RankingScore) extends Scorer(score) with WithParameters {
  override def evaluate[InputTesting, PredictorInstance <: RankingPredictor[PredictorInstance]](
    testing: DataSet[InputTesting],
    predictorInstance: PredictorInstance,
    evaluateParameters: ParameterMap)(implicit
    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, (Int,Int,Double)])
  : DataSet[Double] = {
    //calculate predictions from rankingpredictor
    //give predictions and concrete typed test to score
    //return score value
    null
  }
}

object RankingScorer{
  implicit def defaultRankingTestDataSetPrepareOperation[
  PredictorInstance <: RankingPredictor[PredictorInstance]] =
    new NoPrepareOperation[PredictorInstance, (Int,Int,Double)]()
}
