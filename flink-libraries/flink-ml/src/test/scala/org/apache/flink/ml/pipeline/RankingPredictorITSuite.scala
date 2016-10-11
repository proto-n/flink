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


package org.apache.flink.ml.pipeline

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{FlinkMLTools, ParameterMap}
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.ml.pipeline.{FitOperation, PredictDataSetOperation, Predictor}
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.ml.recommendation.ALS.Factors
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


class RankingPredictorITSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "Evaluation of ranking prediction"
  it should "make ranking predictions correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mockedPredictor = new PredictDataSetOperation[MockPredictor, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockPredictor,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = env.fromCollection(Seq(
        (1,1,0.7),
        (1,2,0.9),
        (1,3,0.8),
        (1,4,1.0),
        (2,1,0.9),
        (2,2,0.8),
        (2,3,1.0),
        (2,4,0.1),
        (2,5,0.9)
      ))
    }
    class MockPredictor extends Predictor[MockPredictor] with RankingPredictor[MockPredictor] {}
    val mockPredictor = new MockPredictor()
    val users = env.fromCollection(Seq(1,2))
    val rankings = mockPredictor.predictRankings(3,users,new ParameterMap)(
      mockedPredictor,
      new TrainingDataGetterOperation[MockPredictor, (Int, Int, Double)] {
        override def getTrainingData(instance: MockPredictor): DataSet[(Int, Int, Double)] =
          env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(3,3,1.0),(3,4,1.0), (3,5,1.0)))
      },
      new ItemsGetterOperation[MockPredictor] {
        override def getItems(instance: MockPredictor): DataSet[Int] =
          env.fromCollection(Seq(1,2,3))
      }
    ).collect()
    rankings.toSet shouldBe Set(
      (1,4,1),
      (1,2,2),
      (1,3,3),
      (2,3,1),
      (2,5,2),
      (2,1,3)
    )
  }
  it should "use default itemsgetter if neccessary" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mockedPredictor = new PredictDataSetOperation[MockPredictor2, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockPredictor2,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = env.fromCollection(Seq(
        (1,1,0.7),
        (1,2,0.9),
        (1,3,0.8),
        (1,4,1.0),
        (2,1,0.9),
        (2,2,0.8),
        (2,3,1.0),
        (2,4,0.1),
        (2,5,0.9)
      ))
    }
    class MockPredictor2 extends Predictor[MockPredictor2] with RankingPredictor[MockPredictor2] {}
    object MockPredictor2{
      implicit val trainingDataGetterOperation = new TrainingDataGetterOperation[MockPredictor2, (Int, Int, Double)] {
        override def getTrainingData(instance: MockPredictor2): DataSet[(Int, Int, Double)] =
          env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(3,3,1.0)))
      }
    }
    val mockPredictor = new MockPredictor2()
    val users = env.fromCollection(Seq(1,2))
    val rankings = mockPredictor.predictRankings(3,users,new ParameterMap)(
      mockedPredictor,
      implicitly,
      implicitly
    ).collect()
    rankings.toSet shouldBe Set(
      (1,4,1),
      (1,2,2),
      (1,3,3),
      (2,3,1),
      (2,5,2),
      (2,1,3)
    )
  }
  it should "exclude pairs from ranking predictions if asked to" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    var calledArguments :DataSet[(Int,Int)] = null
    val mockedPredictor = new PredictDataSetOperation[MockPredictor, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockPredictor,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = {
        calledArguments = input
        env.fromCollection(Seq(
          (1,1,0.7),
          (1,2,0.9),
          (1,3,0.8),
          (1,4,1.0),
          (2,1,0.9),
          (2,2,0.8),
          (2,3,1.0),
          (2,4,0.1),
          (2,5,0.9)
        ))
      }
    }
    class MockPredictor extends Predictor[MockPredictor] with RankingPredictor[MockPredictor] {}
    val mockPredictor = new MockPredictor()
    val users = env.fromCollection(Seq(1,2))
    val par = new ParameterMap
    val rankings = mockPredictor.predictRankings(3,users,par)(
      mockedPredictor,
      new TrainingDataGetterOperation[MockPredictor, (Int, Int, Double)] {
        override def getTrainingData(instance: MockPredictor): DataSet[(Int, Int, Double)] =
          env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(2,3,1.0)))
      },
      new ItemsGetterOperation[MockPredictor] {
        override def getItems(instance: MockPredictor): DataSet[Int] =
          env.fromCollection(Seq(1,2,3,4,5))
      }
    ).collect()
    calledArguments.collect().toSet shouldBe Set(
      (1,1),
      (1,2),
      (1,3),
      (1,4),
      (1,5),
      (2,1),
      (2,2),
      (2,4),
      (2,5)
    )
    par.add(RankingPredictor.ExcludeKnown, false)
    val rankings2 = mockPredictor.predictRankings(3,users,par)(
      mockedPredictor,
      new TrainingDataGetterOperation[MockPredictor, (Int, Int, Double)] {
        override def getTrainingData(instance: MockPredictor): DataSet[(Int, Int, Double)] =
          env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(2,3,1.0)))
      },
      new ItemsGetterOperation[MockPredictor] {
        override def getItems(instance: MockPredictor): DataSet[Int] =
          env.fromCollection(Seq(1,2,3,4,5))
      }
    ).collect()
    calledArguments.collect().toSet shouldBe Set(
      (1,1),
      (1,2),
      (1,3),
      (1,4),
      (1,5),
      (2,1),
      (2,2),
      (2,3),
      (2,4),
      (2,5)
    )
  }
}
