package com.rison.bigdata.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.bean.TableProcess;
import com.rison.bigdata.common.BigdataConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.function
 * @NAME: TableProcessFunction
 * @USER: Rison
 * @DATE: 2022/9/3 22:33
 * @PROJECT_NAME: flink-realtime-demo
 **/
@Slf4j
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<com.alibaba.fastjson.JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(BigdataConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(BigdataConfig.PHOENIX_SERVER);
    }


    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //TODO 1. get state data
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //TODO 2. filter fields
            JSONObject after = jsonObject.getJSONObject("after");
            filterColumn(after, tableProcess.getSinkColumns());

            //TODO 3. split flow
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                readOnlyContext.output(objectOutputTag, jsonObject);
            }
        } else {
            log.info("该组合key:{}不存在", key);
        }
    }

    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fields = sinkColumns.split("\\,");
        List<String> strings = Arrays.asList(fields);
        after.entrySet().removeIf(next -> !strings.contains(next.getKey()));
    }

    /**
     * value:{"db":"","tn":"","before":{},"after":{},"type":""}
     *
     * @param value
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        //TODO 1. get data and parse  data
        JSONObject jsonObject = JSON.parseObject(value);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);

        //TODO 2. create table
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        //TODO 3. writer state and broadcast
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    /**
     * check table status and create table
     * create table sql: create table if not exists db.tn(id varchar primary key, tm_name varchar) xxx;
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ")
                .append(BigdataConfig.HBASE_SCHEMA)
                .append(".").append(sinkTable)
                .append("(");
        String[] fields = sinkColumns.split("\\,");
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].equals(sinkPk)) {
                sql.append(fields[i]).append(" varchar primary key ");
            } else {
                sql.append(fields[i]).append(" varchar ");
            }
            if (i < fields.length - 1) {
                sql.append(",");
            }
        }
        sql.append(" )").append(sinkExtend);

        log.info(sql.toString());

        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }
}
