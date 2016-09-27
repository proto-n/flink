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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.pipeline.Predictor
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import java.io.Serializable

import org.apache.flink.api.common.operators.Order
import org.apache.flink.ml.pipeline.Predictor
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.math.log

/**
  * Created by kd on 9/27/16.
  */

class RankingRecommendationScores(val topK : Int) {
  def predictScores(als: Predictor[ALS], userItemPairs: DataSet[(Int,Int)]): DataSet[(Int,Int,Double)] = {
    als.predict(userItemPairs)
  }

  def getUserItemPairs(users : DataSet[Int], items : DataSet[Int], exclude : DataSet[(Int,Int)]) : DataSet[(Int,Int)] = {
    users.cross(items)
      .leftOuterJoin(exclude).where(0,1).equalTo(0,1)
      .apply((l,r,o:Collector[(Int,Int)])=>{
        Option(r) match {
          case Some(_) => ()
          case None => o.collect(l)
        }
      })
  }

  def predictions(als:Predictor[ALS], users : DataSet[Int], items : DataSet[Int], exclude : DataSet[(Int,Int)]): DataSet[(Int, Int, Int)] = {
    val userItemPairs = getUserItemPairs(users,items,exclude)
    val topKcopy = topK
    predictScores(als, userItemPairs)
      .groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .reduceGroup( _.zip((1 to topKcopy).toIterator) )
      .flatMap( _.map { case ((u,i,pred),rank) => (u,i,rank) } )
  }

  def idcgs(test: DataSet[(Int,Int,Double)]): DataSet[(Int, Double)] = {
    val topKcopy = topK
    test
      .groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .reduceGroup(elements => {
        val bufferedElements = elements.buffered
        val user = bufferedElements.head._1
        val idcg = bufferedElements
          .map(_._3)
          .zip((1 to topKcopy).toIterator)
          .map { case (rel, rank) => rel / (scala.math.log(rank + 1) / scala.math.log(2)) }
          .sum

        (user, idcg)
      })
  }

  def dcgs(predictions: DataSet[(Int,Int,Int)], test: DataSet[(Int,Int,Double)]) : DataSet[(Int,Double)] = predictions
    .leftOuterJoin(test)
    .where(0, 1)
    .equalTo(0, 1)
    .apply((l,r) => (l,Option(r)))
    .groupBy(_._1._1)
    .reduceGroup( elements => {
      val bufferedElements = elements.buffered
      val user = bufferedElements.head._1._1
      val dcg : Double = bufferedElements
        .map {
          case ((u1, i1, rank), Some((u2, i2, rel))) =>
            rel / (scala.math.log(rank + 1) / scala.math.log(2))
          case (l, None) =>
            0.0
        }
        .sum
      (user, dcg)
    })

  def ndcgs(dcgs:DataSet[(Int,Double)], idcgs:DataSet[(Int,Double)]) : DataSet[(Int,Double)] = idcgs
    .join(dcgs)
    .where(0)
    .equalTo(0)
    .apply((idcg,dcg)=>(idcg._1, dcg._2/idcg._2))

  def averageNdcg(rankingPredictions : DataSet[(Int,Int,Int)], test : DataSet[(Int,Int,Double)]) : Double = {
    val idcgsV = idcgs(test)
    val dcgsV = dcgs(rankingPredictions, test)
    val ndcgsV = ndcgs(dcgsV, idcgsV)

    ndcgsV.map(_._2).mean().collect().head
  }

  def joinWithTest(rankingPredictions : DataSet[(Int,Int,Int)], test : DataSet[(Int,Int,Double)]) : DataSet[(Int,Int,Option[Int],String)] = {
    rankingPredictions.fullOuterJoin(test).where(0,1).equalTo(0,1).apply((l,r,o:Collector[(Int,Int,Option[Int],String)]) => {
      (Option(l), Option(r)) match {
        case (None, Some(rr)) => o.collect((rr._1, rr._2, None, "false_negative"))
        case (Some(ll), None) => o.collect((ll._1, ll._2, Some(ll._3), "false_positive"))
        case (Some(ll), Some(rr)) => o.collect((ll._1, ll._2, Some(ll._3), "true_positive"))
        case default => ()
      }
    })
  }

  def countTypes(joined : DataSet[(Int,Int,Option[Int],String)]) : DataSet[(Int,Int,Int,Int)] = {
    joined
      .groupBy(0)
      .reduceGroup(elements => {
        val bufferedElements = elements.buffered
        val user=bufferedElements.head._1
        var truePositive, falsePositive, falseNegative=0
        for((_,_,_,t)<-bufferedElements){
          t match {
            case "true_positive" => truePositive+=1
            case "false_positive" => falsePositive+=1
            case "false_negative" => falseNegative+=1
          }
        }
        (user,truePositive,falsePositive,falseNegative)
      })
  }


  def countTypesUpToK(joined : DataSet[(Int,Int,Option[Int],String)]) : DataSet[(Int,Int,Int,Int,Int)] = joined
    .map(x=>x match {
      case (user,item,Some(rank),t)=>(user,item,rank,t)
      case (user,item,None,t)=>(user,item,0,t)
    })
    .groupBy(0)
    .sortGroup(2, Order.ASCENDING)
    .reduceGroup((elements,collector:Collector[(Int,Int,Int,Int,Int)]) => {
      val bufferedElements = elements.buffered
      val user=bufferedElements.head._1
      var truePositive, falsePositive, falseNegative=0
      for((_,_,rank,t)<-bufferedElements) {
        t match {
          case "true_positive" => truePositive += 1
          case "false_positive" => falsePositive += 1
          case "false_negative" => falseNegative += 1
        }
        if(rank != 0){
          collector.collect((user,rank,truePositive,falsePositive,falseNegative))
        }
      }
    })

  def precisions(rankingPredictions : DataSet[(Int,Int,Int)], test : DataSet[(Int,Int,Double)]) : DataSet[(Int,Double)] = {
    val countedPerUser = countTypes(joinWithTest(rankingPredictions,test))
    countedPerUser.map(x=>x match {
      case (user, truePositive, falsePositive, falseNegative) => (user, truePositive.toDouble/(truePositive.toDouble+falsePositive.toDouble))
    })
  }
  def recalls(rankingPredictions : DataSet[(Int,Int,Int)], test : DataSet[(Int,Int,Double)]) : DataSet[(Int,Double)] = {
    val countedPerUser = countTypes(joinWithTest(rankingPredictions,test))
    countedPerUser.map(x=>x match {
      case (user, truePositive, falsePositive, falseNegative) => (user, truePositive.toDouble/(truePositive.toDouble+falseNegative.toDouble))
    })
  }
}
