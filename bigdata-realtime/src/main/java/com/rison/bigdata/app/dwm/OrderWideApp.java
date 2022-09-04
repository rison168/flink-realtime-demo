package com.rison.bigdata.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.app.function.DimAsyncFunction;
import com.rison.bigdata.bean.OrderDetail;
import com.rison.bigdata.bean.OrderInfo;
import com.rison.bigdata.bean.OrderWide;
import com.rison.bigdata.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.dwm
 * @NAME: OrderWideApp
 * @USER: Rison
 * @DATE: 2022/9/4 21:11
 * @PROJECT_NAME: flink-realtime-demo
 * //数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm)
 * //程  序：         MockDb               -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(Redis) -> Kafka
 **/
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. get flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //set flink checkpoint and backend
        env.setStateBackend(new FsStateBackend("hdfs:///rison/bigdata/orderWide/ck"));
        env.enableCheckpointing(5_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1_0000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // TODO 2. 读取kafka 主题数据， 并装换 为JavaBean 对象 提取时间戳生产waterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_01";
        String servers = "tbds-172-16-16-87:6669";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(KafkaUtil.<String>getKafkaConsumer(orderInfoSourceTopic, groupId, "earliest", servers))
                .map(data -> {
                    OrderInfo orderInfo = JSON.parseObject(data, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dataTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dataTimeArr[0]);
                    orderInfo.setCreate_date(dataTimeArr[1].split(":")[0]);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        })
                )
                ;

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(KafkaUtil.<String>getKafkaConsumer(orderDetailSourceTopic, groupId, "earliest", servers))
                .map(data -> {
                    OrderDetail orderDetail = JSON.parseObject(data, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
                );

        //TODO 3. 双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getId))
                //生成环境给的最大延迟时间
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideNoDimDS.print("orderWideNoDimDS:");

        //TODO 4. 关联维度信息 Hbase Phoenix
        //关联用户维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideNoDimDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setUser_gender(dimInfo.getString("GENDER"));
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                        input.setUser_age((int) age);
                    }
                },
                60, TimeUnit.SECONDS);
        //关联地区维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                },
                60, TimeUnit.SECONDS);

        //关联SKU维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //关联SPU维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //关联TM维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //关联Category维度
        final SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS:");

        //TODO 5. 将数据写到kafka
        orderWideWithCategory3DS.map(JSONObject::toJSONString)
                .addSink(KafkaUtil.<String>getKafkaProducer(orderWideSinkTopic, servers));

        env.execute(OrderWideApp.class.getName());
    }
}
