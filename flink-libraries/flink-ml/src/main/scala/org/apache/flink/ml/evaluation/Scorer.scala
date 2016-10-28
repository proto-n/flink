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
  PredictorType[PredictorInstanceType] <: WithParameters,
  PreparedTesting
] (val score: AbstractScore[PredictorType, PreparedTesting])
  extends WithParameters {
  def evaluate[InputTesting, PredictorInstance <: PredictorType[PredictorInstance]](
      testing: DataSet[InputTesting],
      predictorInstance: PredictorInstance,
      evaluateParameters: ParameterMap = ParameterMap.Empty)(implicit
    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, PreparedTesting]):
    DataSet[Double] = {

    val resultingParameters = predictorInstance.parameters ++ evaluateParameters
    val predictions = prepareOperation.prepare(predictorInstance, testing, resultingParameters)
    score.eval(predictions)
  }
}

class PairwiseScorer(override val score: PairwiseScore[Double])
  extends Scorer(score) with WithParameters
{
//
//  override def evaluate[InputTesting, PredictorInstance <: Predictor[PredictorInstance]](
//    testing: DataSet[InputTesting],
//    predictorInstance: PredictorInstance,
//    evaluateParameters: ParameterMap)(implicit
//    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, DataSet[(Double, Double)]])
//  : DataSet[Double] = {
//    val resultingParameters = predictorInstance.parameters ++ evaluateParameters
//    val predictions = prepareOperation.prepare(predictorInstance, testing, resultingParameters)
//    score.evaluate(predictions)
//  }
}

class RankingScorer(override val score: RankingScore) extends Scorer(score) with WithParameters {
//  override def evaluate[InputTesting, PredictorInstance <: RankingPredictor[PredictorInstance]](
//    testing: DataSet[InputTesting],
//    predictorInstance: PredictorInstance,
//    evaluateParameters: ParameterMap)(implicit
//    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, (Int,Int,Double)])
//  : DataSet[Double] = {
//    val resultingParameters = predictorInstance.parameters ++ evaluateParameters
//    // preparing testing DataSet to contain (Int,Int,Double)
//    val preparedTesting = prepareOperation.prepare(predictorInstance, testing, resultingParameters)
//    // giving ranking predictions
//    val predictions = predictorInstance.evaluateRankings(preparedTesting, resultingParameters)
//    // evaluation comparing the predicted rankings with the true ratings
//    score.evaluate(predictions, preparedTesting)
//  }
}

object RankingScorer {

  def rankingEvalPrepare[Instance <: RankingPredictor[Instance]] =
    new PrepareOperation[Instance,
      (Int,Int,Double), (DataSet[(Int,Int,Double)], DataSet[(Int,Int,Int)])] {
      override def prepare(als: Instance,
                           test: DataSet[(Int, Int, Double)],
                           parameters: ParameterMap):
      (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]) = {
        (test, als.evaluateRankings(test, parameters))
      }
    }
}
