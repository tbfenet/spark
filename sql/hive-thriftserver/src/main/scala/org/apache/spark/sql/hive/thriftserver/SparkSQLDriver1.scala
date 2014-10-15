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

package org.apache.spark.sql.hive.thriftserver

import java.sql.SQLException
import java.util.{ArrayList => JArrayList, Properties}

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.SerDeUtils
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, ObjectInspector}
import org.apache.spark.Logging
import org.apache.spark.sql._

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.{MapType, ArrayType, StructType}
import org.apache.spark.sql.execution.{SparkPlan, Command}
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}

import scala.collection.JavaConversions._

import org.apache.spark.sql.execution.{Command => PhysicalCommand}
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand


private[hive] class SparkSQLDriver1(val context: HiveContext = SparkSQLEnv.hiveContext)
  extends Driver with Logging {

  private var tableSchema: Schema = _
  private var hiveResponse: Seq[String] = _

  override def init(): Unit = {
  }

  private def getResultSetSchema(query: context.QueryExecution): Schema = {
    val analyzed = query.analyzed
    logDebug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.size == 0) {
      new Schema(new FieldSchema("Response code", "string", "") :: Nil, null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }

      new Schema(fieldSchemas, null)
    }
  }

  override def run(command: String): CommandProcessorResponse = {
    // TODO unify the error code
    try {
      val execution = context.executePlan(context.sql(command).logicalPlan)
      val res = execution.stringResult()

      hiveResponse = execution.executedPlan.
      tableSchema = getResultSetSchema(execution)
      new CommandProcessorResponse(0)
    } catch {
      case cause: Throwable =>
        logError(s"Failed in [$command]", cause)
        new CommandProcessorResponse(-3, ExceptionUtils.getFullStackTrace(cause), null)
    }
  }




  /**
   * Returns the result as a hive compatible sequence of strings.  For native commands, the
   * execution is simply passed back to Hive.
   */
  def stringResult(executedPlan: SparkPlan): Seq[String] = executedPlan match {
    case describeHiveTableCommand: DescribeHiveTableCommand =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      describeHiveTableCommand.hiveString
    case command: PhysicalCommand =>
      command.sideEffectResult.map(_.toString)

    case other =>

      val result = executedPlan.executeCollect()


      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      val asString = result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq
      asString
  }


  def serialiseRow(plan:StructType,row: Row):String ={




    val rowAsList = row.toList



  }

  def objectInspector(data:Seq[(Any,DataType)]):(Any,ObjectInspector)  = {




    val data.map {
      case (struct: Row, StructType(fields)) =>
        objectInspector( struct.zip(fields).map {

          case (v,t) => (v,t.dataType)

        }               )
      case (seq: Seq[_], ArrayType(typ, _)) =>
        (seq.toArray,ObjectInspectorFactory.getStandardListObjectInspector())
      case (map: Map[_,_], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }



  }


  private def initSerde( schema: List[FieldSchema]) {





      val names: String = schema.map(_.getName ).mkString(",")
      val types: String = schema.map(_.getType).mkString(",")
     val  serde = new LazySimpleSerDe
      val props: Properties = new Properties
      if (names.length > 0) {
        logDebug("Column names: " + names)
        props.setProperty(serdeConstants.LIST_COLUMNS, names)
      }
      if (types.length > 0) {
        logDebug("Column types: " + types)
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types)
      }
      serde.initialize( new Configuration, props)

    serde
  }

  def toObjectInspector(dAt:(Any,DataType))={
    case
  }


  override def close(): Int = {
    hiveResponse = null
    tableSchema = null
    0
  }

  override def getSchema: Schema = tableSchema

  override def getResults(res: JArrayList[String]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.addAll(hiveResponse)
      hiveResponse = null
      true
    }
  }

  override def destroy() {
    super.destroy()
    hiveResponse = null
    tableSchema = null
  }
}
