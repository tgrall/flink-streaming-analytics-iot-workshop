package com.mapr.demos;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
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


    TypeInformation<Row> typeInfo = Types.ROW(
            new String[] { "mac", "amplitude"},
            new TypeInformation<?>[] {Types.STRING(), Types.INT()}
    );
    

    KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSourceWithTime(
            "/apps/iot_stream:sensor-json",
            properties,
            typeInfo,
            "proctime");

    String sql = "SELECT mac, avg(amplitude) from sensors GROUP BY TUMBLE(proctime, INTERVAL '5' SECOND), mac ";
    tableEnv.registerTableSource("sensors", kafkaTableSource);
    Table result = tableEnv.sql(sql);

    DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
    resultSet.print();

    env.execute();
  }


}
