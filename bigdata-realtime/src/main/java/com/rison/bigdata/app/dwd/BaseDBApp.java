package com.rison.bigdata.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.rison.bigdata.app.function.CustomerDeserialization;
import com.rison.bigdata.app.function.DimSinkFunction;
import com.rison.bigdata.app.function.TableProcessFunction;
import com.rison.bigdata.bean.TableProcess;
import com.rison.bigdata.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.dwd
 * @NAME: BaseDBApp
 * @USER: Rison
 * @DATE: 2022/9/3 21:53
 * @PROJECT_NAME: flink-realtime-demo
 * data flow : web/app -> nginx -> SparingBoot -> Mysql -> FLinkAPP -> Kafka(ODS)
 * -> FlinkAPP -> Kafka(dwd)/Phoenix
 **/
public class BaseDBApp {

    public static void main(String[] args) throws Exception {
        // TODO 1. get flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //set flink checkpoint and backend
        env.setStateBackend(new FsStateBackend("hdfs:///rison/bigdata/basedb/ck"));
        env.enableCheckpointing(5_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1_0000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        //TODO 2. 消费kafka ods_base_db 数据
        String topic = "ods_base_db";
        String groupId = "ods_base_db_group_01";
        String servers = "tbds-172-16-16-87:6669";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.<String>getKafkaConsumer(topic, groupId, "earliest", servers));

        //TODO 3. 将每行的数据装换成JSON对象，过滤（delete）数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(json -> JSON.parseObject(json))
                .filter(jsonObject -> {
                    String type = jsonObject.getString("type");
                    if (type.equals("delete")) {
                        return false;
                    } else {
                        return true;
                    }
                });

        //TODO 4. 使用FlinkCDC 消费配置表，并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("")
                .port(3306)
                .username("root")
                .password("")
                .databaseList("")
                .tableList("")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> stringTableProcessMapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(stringTableProcessMapStateDescriptor);

        //TODO 5. 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6. 分流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };

        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, stringTableProcessMapStateDescriptor));

        //TODO 7. 提取hbase数据
        DataStream<JSONObject> hbaseDS = kafka.getSideOutput(hbaseTag);
        kafka.print("kafka:");
        hbaseDS.print("hbase:");

        //TODO 8. 将hbase 数据写到hbase
        hbaseDS.addSink(new DimSinkFunction());
        //TODO 9. 将kafka数据写到kafka
        kafka.addSink(KafkaUtil.<JSONObject>getKafkaProducer(
                "default_topic",
                "tbds-172-16-16-87:6669",
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                                jsonObject.getString("after").getBytes());
                    }
                }
                )
        );

        env.execute(BaseDBApp.class.getName());
    }
}
