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

import java.io.File
import java.net.ServerSocket
import java.sql.{ResultSet, DriverManager, Statement}
import java.util.concurrent.TimeoutException

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.jdbc.HiveDriver
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.sys.process.{Process, ProcessLogger}

/**
 * Tests for the HiveThriftServer2 using JDBC.
 */
class SharkServer2Suite extends FunSuite with Logging {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  private val listeningHost = "localhost"
  private val listeningPort =  {
    // Let the system to choose a random available port to avoid collision with other parallel
    // builds.
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private val warehousePath = getTempFilePath("warehouse")
  private val metastorePath = getTempFilePath("metastore")
  private val metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"

  def startThriftServerWithin(timeout: FiniteDuration = 60.seconds)(f: Statement => Unit) {
    val serverScript = "../../sbin/start-sharkserver.sh".split("/").mkString(File.separator)

/*

    val defaultArgs = Seq("./bin/shark", "--service", "sharkserver",
      "--verbose",
      "-p",
      PORT,
      "--hiveconf",
      "hive.root.logger=INFO,console",
      "--hiveconf",
      "\"javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true\"",
      "--hiveconf",
      "\"hive.metastore.warehouse.dir=" + WAREHOUSE_PATH + "\"")
*/

    val command =
      s"""$serverScript
         |  --master local
         |  --hiveconf hive.root.logger=INFO,console
         |  --hiveconf javax.jdo.option.ConnectionURL=$metastoreJdbcUri
         |  --hiveconf hive.metastore.warehouse.dir=$warehousePath
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=$listeningHost
         |  -p $listeningPort
       """.stripMargin.split("\\s+").toSeq

    val serverStarted = Promise[Unit]()
    val buffer = new ArrayBuffer[String]()

    def captureOutput(source: String)(line: String) {
      System.err.println(line)
      buffer += s"$source> $line"
      if (line.contains("Starting Shark server")) {
        serverStarted.success(())
      }
    }

    val process = Process(command).run(
      ProcessLogger(captureOutput("stdout"), captureOutput("stderr")))

    Future {
      val exitValue = process.exitValue()
      logInfo(s"Spark SQL Thrift server process exit value: $exitValue")
    }

    val jdbcUri = s"jdbc:hive://$listeningHost:$listeningPort/"
    val user = System.getProperty("user.name")

    try {
      Await.result(serverStarted.future, timeout)

      val connection = DriverManager.getConnection(jdbcUri, user, "")
      val statement = connection.createStatement()

      try {
        f(statement)
      } finally {
        statement.close()
        connection.close()
      }
    } catch {
      case cause: Exception =>
        cause match {
          case _: TimeoutException =>
            logError(s"Failed to start Hive Thrift server within $timeout", cause)
          case _ =>
        }
        logError(
          s"""
             |=====================================
             |HiveThriftServer2Suite failure output
             |=====================================
             |HiveThriftServer2 command line: ${command.mkString(" ")}
             |JDBC URI: $jdbcUri
             |User: $user
             |
             |${buffer.mkString("\n")}
             |=========================================
             |End HiveThriftServer2Suite failure output
             |=========================================
           """.stripMargin, cause)
        fail()
    } finally {
      warehousePath.delete()
      metastorePath.delete()
      process.destroy()
    }
  }

  test("Test JDBC query execution") {
    startThriftServerWithin() { statement =>
      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

      val queries = Seq(
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test")

      queries.foreach(statement.execute)

      assertResult("test", "Table exists") {
        val resultSet = statement.executeQuery("show tables")
        resultSet.next()
        resultSet.getString(1)
      }


      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }

      assertResult(List((238,"val_238"), (86,"val_86"), (311,"val_311"), (27,"val_27"), (165,"val_165")), "contains data") {
        val resultSet = statement.executeQuery("SELECT key,val FROM test")
        var out:List[(Int,String)] = Nil
        while(resultSet.next()) {

          out= ( resultSet.getInt(1),resultSet.getString(2)) ::out
        }
        out.reverse
      }

    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    startThriftServerWithin() { statement =>
      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource(
          "data/files/small_kv_with_null.txt")

      val queries = Seq(
        "DROP TABLE IF EXISTS test_null",
        "CREATE TABLE test_null(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test_null")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")

      (0 until 5).foreach { _ =>
        resultSet.next()
        assert(resultSet.getInt(1) === 0)
        assert(resultSet.wasNull())
      }

      assert(!resultSet.next())
    }
  }
}
