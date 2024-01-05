package io.tidb.bigdata.flink.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Ignore;
import org.junit.Test;

public class TiDBCatalogTest {

  @Ignore
  @Test
  public void test1() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    tableEnvironment.executeSql(
        "CREATE CATALOG `tidb`\n"
            + "WITH (\n"
            + "    'type' = 'tidb',\n"
            + "    'tidb.database.url' = 'jdbc:mysql://localhost:4000/test',\n"
            + "    'tidb.username' = 'root',\n"
            + "    'tidb.password' = ''\n"
            + ")");
    tableEnvironment.executeSql("SELECT * FROM `tidb`.`test`.`tbsplit`").print();
  }

  @Ignore
  @Test
  public void test2() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    tableEnvironment.executeSql(
        "CREATE CATALOG `tidb`\n"
            + "WITH (\n"
            + "    'type' = 'tidb',\n"
            + "    'tidb.database.url' = 'jdbc:mysql://localhost:4000/test',\n"
            + "    'tidb.username' = 'root',\n"
            + "    'tidb.password' = ''\n"
            + ")");
    TiDBCatalog tiDBCatalog = (TiDBCatalog) tableEnvironment.getCatalog("tidb").get();
    String createTableSql =
        "CREATE TABLE IF NOT EXISTS `test`.`test_all_types`\n"
            + "(\n"
            + "    c1  tinyint,\n"
            + "    c2  smallint,\n"
            + "    c3  mediumint,\n"
            + "    c4  int,\n"
            + "    c5  bigint PRIMARY KEY auto_increment,\n"
            + "    c6  char(10),\n"
            + "    c7  varchar(20),\n"
            + "    c8  tinytext,\n"
            + "    c9  mediumtext,\n"
            + "    c10 text,\n"
            + "    c11 longtext,\n"
            + "    c12 binary(20),\n"
            + "    c13 varbinary(20),\n"
            + "    c14 tinyblob,\n"
            + "    c15 mediumblob,\n"
            + "    c16 blob,\n"
            + "    c17 longblob,\n"
            + "    c18 float,\n"
            + "    c19 double,\n"
            + "    c20 decimal(6, 3),\n"
            + "    c21 date,\n"
            + "    c22 time,\n"
            + "    c23 datetime,\n"
            + "    c24 timestamp,\n"
            + "    c25 year,\n"
            + "    c26 boolean,\n"
            + "    c27 json,\n"
            + "    c28 enum ('1','2','3'),\n"
            + "    c29 set ('a','b','c')\n"
            + ")";
    String insertSql =
        "INSERT INTO `test`.`test_all_types`\n"
            + "VALUES (\n"
            + " 1 ,\n"
            + " 1 ,\n"
            + " 1 ,\n"
            + " 1 ,\n"
            + " null ,\n"
            + " 'chartype' ,\n"
            + " 'varchartype' ,\n"
            + " 'tinytexttype' ,\n"
            + " 'mediumtexttype' ,\n"
            + " 'texttype' ,\n"
            + " 'longtexttype' ,\n"
            + " 'binarytype' ,\n"
            + " 'varbinarytype' ,\n"
            + " 'tinyblobtype' ,\n"
            + " 'mediumblobtype' ,\n"
            + " 'blobtype' ,\n"
            + " 'longblobtype' ,\n"
            + " 1.234 ,\n"
            + " 2.456789 ,\n"
            + " 123.456 ,\n"
            + " '2020-08-10' ,\n"
            + " '15:30:29' ,\n"
            + " '2020-08-10 15:30:29' ,\n"
            + " '2020-08-10 16:30:29' ,\n"
            + " 2020 ,\n"
            + " true ,\n"
            + " '{\"a\":1,\"b\":2}' ,\n"
            + " '1' ,\n"
            + " 'a' \n"
            + ")";
    tiDBCatalog.sqlUpdate(createTableSql, insertSql);
    tableEnvironment.executeSql("SELECT * FROM `tidb`.`test`.`test_all_types` WHERE c1 = 1 AND c2 = 1 AND c3 <> 2 ").print();
  }

  @Ignore
  @Test
  public void test3() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);
    tableEnvironment.executeSql(
        "CREATE CATALOG `tidb`\n"
            + "WITH (\n"
            + "  'type' = 'tidb',\n"
            + "  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',\n"
            + "  'tidb.username' = 'root',\n"
            + "  'tidb.password' = '',\n"
            + "  'tidb.streaming.source' = 'kafka',\n"
            + "  'tidb.streaming.codec' = 'canal-protobuf',\n"
            + "  'tidb.streaming.kafka.bootstrap.servers' = 'localhost:9092',\n"
            + "  'tidb.streaming.kafka.topic' = 'test_cdc',\n"
            + "  'tidb.streaming.kafka.group.id' = 'test_cdc_group',\n"
            + "  'tidb.streaming.ignore-parse-errors' = 'true'\n"
            + ");");
    tableEnvironment.executeSql("SELECT * FROM `tidb`.`test`.`test_all_types`").print();
  }
}
