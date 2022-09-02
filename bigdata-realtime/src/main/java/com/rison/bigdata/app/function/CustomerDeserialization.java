package com.rison.bigdata.app.function;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.function
 * @NAME: CustomerDeserialization
 * @USER: Rison
 * @DATE: 2022/9/2 12:34
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","tm_name":""....},
     * "after":{"id":"","tm_name":""....},
     * "type":"c u d",
     * //"ts":156456135615
     * }
     *
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1. create json object
        JSONObject result = new JSONObject();

        //2. get database and table
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        //3. get before data of value
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (beforeJson != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            beforeFields.forEach(field -> {
                        Object beforeValue = before.get(field);
                        beforeJson.put(field.name(), beforeValue);
                    }
            );
        }

        //4. get after data of value
        Struct after = (Struct) value.get("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            afterFields.forEach(field -> {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            });
        }

        //5. get operate type of value
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //6. put the filed to object
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //7. 输出数据
        collector.collect(result.toJSONString());


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
