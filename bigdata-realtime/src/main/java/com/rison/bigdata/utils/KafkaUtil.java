package com.rison.bigdata.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.bigdata.utils
 * @NAME: KafkaUitl
 * @USER: Rison
 * @DATE: 2022/9/2 16:27
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class KafkaUtil {
    /**
     * get FlinkKafkaProducer<T>
     * @param topic
     * @param servers
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(String topic, String servers) {
        return new FlinkKafkaProducer<T>(
                topic,
                (SerializationSchema<T>) new SimpleStringSchema(),
                producerProps(servers)
        );
    }

    /**
     * kafka 消费者 properties
     *
     * @param servers kafka broker servers
     * @param groupId groupId
     * @return
     */
    public static Properties consumerProps(String servers, String groupId, String offsetResetType) {
        if (offsetResetType == null) {
            offsetResetType = "latest";
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", offsetResetType);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "3000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        //设置SASL_PLAINT认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram required username=\"kafka\" password=\"kafka@Tbds.com\";");
        return props;
    }

    /**
     * kafka 生产者 properties
     *
     * @param servers kafka broker servers
     * @return
     */
    public static Properties producerProps(String servers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        //设置SASL_PLAINT认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram required username=\"kafka\" password=\"kafka@Tbds.com\";");
        return props;
    }


}
