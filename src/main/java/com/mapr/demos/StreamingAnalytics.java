package com.mapr.demos;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

  public class StreamingAnalytics {

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    Properties properties = new Properties();
    properties.setProperty("group.id", "flink_consumer_wifi_streaming_analytics");

    String[] names = {"mac", "amplitude"};
    TypeInformation<?>[] types = {
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO
            };

    RowTypeInfo typeInfo = new RowTypeInfo(types, names);

    KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSource(
            "/apps/iot_stream:sensor-json",
            properties,
            typeInfo);

    String sql = "SELECT mac, amplitude from sensors";
    tableEnv.registerTableSource("sensors", kafkaTableSource);
    Table result = tableEnv.sql(sql);

    DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
    resultSet.print();

    env.execute();
  }


}
