package com.mapr.demos;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Created by tgrall on 13/06/2017.
 */
public class Kafka09JsonTableSourceWithTime extends Kafka09JsonTableSource implements DefinedProctimeAttribute{

  private String proctime = "proctime";

  public Kafka09JsonTableSourceWithTime(String topic, Properties properties, TypeInformation<Row> typeInfo, String proctime ) {
    super(topic, properties, typeInfo);
    this.proctime = proctime;
  }


  public Kafka09JsonTableSourceWithTime(String topic, Properties properties, TypeInformation<Row> typeInfo) {
    super(topic, properties, typeInfo);
  }

  @Override
  public String getProctimeAttribute() {
    return proctime;
  }
}
