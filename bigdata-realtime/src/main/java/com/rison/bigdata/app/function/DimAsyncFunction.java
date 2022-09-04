package com.rison.bigdata.app.function;

import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.common.BigdataConfig;
import com.rison.bigdata.utils.DimUtil;
import com.rison.bigdata.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.function
 * @NAME: DimAsyncFunction
 * @USER: Rison
 * @DATE: 2022/9/4 21:42
 * @PROJECT_NAME: flink-realtime-demo
 **/
@Slf4j
public abstract class  DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public abstract String getKey(T input);

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(BigdataConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(BigdataConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //查询主键
                String id = getKey(input);
                //查询维度信息
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                //补充维度信息
                if (dimInfo != null){
                    join(input, dimInfo);
                }
                //数据输出
                resultFuture.complete(Collections.singletonList(input));

            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        log.warn("TimeOut:{}", input);
    }

    public abstract void join(T input, JSONObject dimInfo) throws ParseException;


}
