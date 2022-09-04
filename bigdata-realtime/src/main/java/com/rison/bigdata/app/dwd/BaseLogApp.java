package com.rison.bigdata.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.dwd
 * @NAME: BaseLogApp
 * @USER: Rison
 * @DATE: 2022/9/4 13:06
 * @PROJECT_NAME: flink-realtime-demo
 * 数据流： web/app -> Nginx -> springBoot -> kafka(ods) -> FlinkAPP -> kafka(dwd)
 **/
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("hdfs:///rison/bigdata/baseLog/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(30000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        //TODO 2. 消费ods_base_log 主题数据创建流
        String topic = "ods_base_log";
        String groupId = "base_log_app_group01";
        String servers = "tbds-172-16-16-87:6669";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.<String>getKafkaConsumer(topic, groupId, "earliest", servers));

        //TODO 3. 将每行数据装换为JSON对象
        OutputTag<String> dirtyOutPutTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyOutPutTag, value);
                }
            }
        });
        jsonObjectDS.getSideOutput(dirtyOutPutTag).print();

        //TODO 4. 新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").get("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getIterationRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取数据中的"is_new"标记
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        //判断isNew标记是否为“1”
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                //修改 isNew 标记
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });


        //TODO 5. 分流 侧输出流 页面：主流 启动：侧输出流 曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                // 获取启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    collector.collect(jsonObject.toJSONString());
                    //取出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6. 提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //TODO 7. 将三个流进行打印并输出到对应的主题
        startDS.print("start flow:");
        displayDS.print("display flow:");
        pageDS.print("page flow:");

        startDS.addSink(KafkaUtil.<String>getKafkaProducer("dwd_start_log","tbds-172-16-16-87:6669"));
        displayDS.addSink(KafkaUtil.<String>getKafkaProducer("dwd_display_log","tbds-172-16-16-87:6669"));
        pageDS.addSink(KafkaUtil.<String>getKafkaProducer("dwd_page_log", "tbds-172-16-16-87:6669"));

        env.execute("BaseLogApp");


    }
}
