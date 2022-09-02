package com.rison.bigdata.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.ods
 * @NAME: flinkCDC
 * @USER: Rison
 * @DATE: 2022/9/2 12:16
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class flinkCDC {
    public static void main(String[] args) {
        //TODO 1. create flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2. set checkpoint and back end
        env.setStateBackend(new FsStateBackend("hdfs:///rison/bigdata/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        //TODO 3. 通过FLinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> stringDebeziumSourceFunction = MySQLSource.<String>builder()
                .hostname("")
                .port(3306)
                .username("")
                .password("")
                .databaseList("")
                .deserializer()
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(stringDebeziumSourceFunction);
        //TODO 4.数据写入到Kafka
        dataStreamSource.print();
        dataStreamSource.addSink()


    }
}
