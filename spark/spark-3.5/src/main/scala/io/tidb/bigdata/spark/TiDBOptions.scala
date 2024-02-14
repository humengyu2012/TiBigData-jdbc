/*
 * Copyright 2024 TiDB Project Authors.
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

package io.tidb.bigdata.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.collection.JavaConverters._

object TiDBOptions {
  val TIDB_ENABLE_JDBC_SSL: String = newOption("jdbc.tls_enable")
  val TIDB_JDBC_CLIENT_CERT_STORE: String = newOption("jdbc.client_cert_store")
  val TIDB_JDBC_CLIENT_CERT_PASSWORD: String = newOption("jdbc.client_cert_password")
  val TIDB_JDBC_SERVER_CERT_STORE: String = newOption("jdbc.server_cert_store")
  val TIDB_JDBC_SERVER_CERT_PASSWORD: String = newOption("jdbc.server_cert_password")
  private val TIDB_ADDRESS: String = newOption("tidb.addr")
  private val TIDB_PORT: String = newOption("tidb.port")
  private val TIDB_USER: String = newOption("tidb.user")
  private val TIDB_PASSWORD: String = newOption("tidb.password")
  private val TIDB_DATABASE: String = newOption("database")
  private val TIDB_TABLE: String = newOption("table")
  private val optParamPrefix = "spark.tispark."

  def apply(from: CaseInsensitiveMap[String]): JDBCOptions = new JDBCOptions(jdbcOptions(from))

  def jdbcOptions(from: CaseInsensitiveMap[String]): CaseInsensitiveMap[String] = populateJDBCOptions(checkNotSupportedOptions(from))

  private def checkNotSupportedOptions(parameters: CaseInsensitiveMap[String]): CaseInsensitiveMap[String] = {
    if (getOrDefault(parameters, JDBCOptions.JDBC_QUERY_STRING, "").trim.nonEmpty) {
      throw new IllegalArgumentException(
        s"The ${JDBCOptions.JDBC_QUERY_STRING} option is not supported in TiDB spark connector.")
    }
    if (getOrDefault(parameters, JDBCOptions.JDBC_PREPARE_QUERY, "").trim.nonEmpty) {
      throw new IllegalArgumentException(
        s"The ${JDBCOptions.JDBC_PREPARE_QUERY} option is not supported in TiDB spark connector.")
    }
    if (getOrDefault(parameters, JDBCOptions.JDBC_TABLE_NAME, "").trim.isEmpty) {
      throw new IllegalArgumentException(
        s"The ${JDBCOptions.JDBC_TABLE_NAME} option is required in TiDB spark connector.")
    }
    parameters
  }

  private def getOrDefault(parameters: CaseInsensitiveMap[String], name: String, default: String): String = {
    if (parameters.isDefinedAt(name)) {
      parameters(name)
    } else if (parameters.isDefinedAt(s"$optParamPrefix$name")) {
      parameters(s"$optParamPrefix$name")
    } else {
      default
    }
  }

  private def populateJDBCOptions(from: CaseInsensitiveMap[String]): CaseInsensitiveMap[String] = {
    var parameters: CaseInsensitiveMap[String] = mergeWithSparkConf(from);
    val address: String = getOrNull(parameters, TIDB_ADDRESS)
    val port: String = getOrNull(parameters, TIDB_PORT)
    val user: String = getOrNull(parameters, TIDB_USER)
    val password: String = getOrNull(parameters, TIDB_PASSWORD)
    val database: String = getOrNull(parameters, TIDB_DATABASE)
    var table: String = getOrNull(parameters, TIDB_TABLE)

    if (table != null) {
      if (database != null) {
        table = s"$database.$table"
      }
      parameters = parameters + (JDBCOptions.JDBC_TABLE_NAME, table)
    }

    if (address == null || port == null || user == null || password == null || database == null) {
      return parameters;
    }

    var SSLParameters: String = getOrDefault(parameters, TIDB_ENABLE_JDBC_SSL, "false")
    if (SSLParameters.equals("true")) {
      val FILE_PREFIX = "file:"
      var clientCertStore = getOrDefault(parameters, TIDB_JDBC_CLIENT_CERT_STORE, "")
      val clientCertPassword = getOrDefault(parameters, TIDB_JDBC_CLIENT_CERT_PASSWORD, "")
      var serverCertStore = getOrDefault(parameters, TIDB_JDBC_SERVER_CERT_STORE, "")
      val serverCertPassword = getOrDefault(parameters, TIDB_JDBC_SERVER_CERT_PASSWORD, "")
      // Set up Server authentication
      if (serverCertStore.equals("")) {
        SSLParameters = "true&requireSSL=true&verifyServerCertificate=false"
      } else {
        if (!serverCertStore.startsWith(FILE_PREFIX))
          serverCertStore = FILE_PREFIX + serverCertStore
        SSLParameters =
          "true&requireSSL=true&verifyServerCertificate=true&trustCertificateKeyStoreUrl=" +
            serverCertStore + "&trustCertificateKeyStorePassword=" + serverCertPassword
      }
      // Setting up client authentication
      if (!clientCertStore.equals("")) {
        if (!clientCertStore.startsWith(FILE_PREFIX))
          clientCertStore = FILE_PREFIX + clientCertStore
        SSLParameters += "&clientCertificateKeyStoreUrl=" + clientCertStore +
          "&clientCertificateKeyStorePassword=" + clientCertPassword
      }
    }

    val url: String =
      s"jdbc:mysql://address=(protocol=tcp)(host=$address)(port=$port)/?user=$user&password=$password&useSSL=$SSLParameters&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.2,TLSv1.3"
        .replaceAll("%", "%25")
    parameters = parameters + (JDBCOptions.JDBC_URL -> url)
    parameters
  }

  private def mergeWithSparkConf(parameters: CaseInsensitiveMap[String]): CaseInsensitiveMap[String] = {
    val sparkConf = SparkContext.getOrCreate().getConf
    // priority: data source config > spark config
    val confMap = sparkConf.getAll.toMap
    checkTiDBPassword(confMap)
    CaseInsensitiveMap(confMap ++ parameters)
  }

  private def checkTiDBPassword(conf: Map[String, String]): Unit = {
    val TIDB_PASSWORD_WITH_PREFIX: String = optParamPrefix + TIDB_PASSWORD
    conf.foreach {
      case (k, _) =>
        if (TIDB_PASSWORD.equals(k) || TIDB_PASSWORD_WITH_PREFIX.equals(k)) {
          throw new IllegalStateException(
            "!Security! Please DO NOT add TiDB password to SparkConf which will be shown on Spark WebUI!")
        }
    }
  }

  private def getOrNull(parameters: CaseInsensitiveMap[String], name: String): String = {
    getOrDefault(parameters, name, null)
  }

  def jdbcOptions(from: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    var caseInsensitiveMap: CaseInsensitiveMap[String] = CaseInsensitiveMap(from.asCaseSensitiveMap().asScala.toMap)
    new CaseInsensitiveStringMap(populateJDBCOptions(checkNotSupportedOptions(caseInsensitiveMap)).toMap.asJava)
  }

  private def newOption(name: String): String = {
    name.toLowerCase(Locale.ROOT)
  }
}