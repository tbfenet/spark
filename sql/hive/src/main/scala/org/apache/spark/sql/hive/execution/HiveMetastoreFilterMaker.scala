/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.MetastoreRelation
import org.apache.spark.sql.types._

case class HiveMetastoreFilterMaker(relation: MetastoreRelation) {

  private[this] def mkMetaPartitionsSpecOr(or: Expression): List[Map[String, String]] = {
    or match {
      case Or(left, right) =>
        mkMetaPartitionsSpecOr(left) ++ mkMetaPartitionsSpecOr(right)
      case x: Expression =>
        List(mkMetaPartitionsSpecAndEq(x))
      case _ =>
        List(Map.empty[String, String])
    }
  }

  private[this] def mapEntry(attr: AttributeReference, value: String): Map[String, String] = {
    Map(relation.attributeMap.get(attr).getOrElse(attr).name -> value)
  }

  private[this] def castToString(value: Literal, via: DataType): String = {
    Cast(Literal.create(Cast(value, via).eval(null), via), StringType).eval(null).toString
  }

  private[this] def castToString(value: Literal): String = {
    Cast(value, StringType).eval(null).toString
  }

  private[this] def mapEntry(attr: AttributeReference, literal: Literal): Map[String, String] = {

    if (attr.dataType == StringType) {
      literal match {
        case Literal(_, StringType) =>
          mapEntry(attr, castToString(literal))
        case Literal(_, DateType) =>
          mapEntry(attr, castToString(literal))
        case _ =>
          Map.empty
      }
    } else if (attr.dataType == DateType) {
      mapEntry(attr, castToString(literal, attr.dataType))
    } else {
      Map.empty
    }
  }

  private[this] def mkMetaPartitionsSpecAndEq(exp: Expression): Map[String, String] = {
    exp match {
      case And(left, right) =>
        mkMetaPartitionsSpecAndEq(left) ++ mkMetaPartitionsSpecAndEq(right)
      case EqualTo(left: AttributeReference, right: Literal) =>
        mapEntry(left, right)
      case EqualTo(left: Literal, right: AttributeReference) =>
        mapEntry(right, left)
      case EqualTo(Cast(attr: AttributeReference, dataType), right: Literal) =>
        mapEntry(attr, right)
      case EqualTo(left: Literal, Cast(attr: AttributeReference, dataType)) =>
        mapEntry(attr, left)
      case _ =>
        Map.empty
    }
  }

  def mkMetaPartitionsSpec(exp: Expression): List[Map[String, String]] = {
    val out = mkMetaPartitionsSpecOr(exp)
    if (out.nonEmpty && out.forall(_.nonEmpty)) {
      out
    } else {
      List(Map.empty[String, String])
    }
  }
}
