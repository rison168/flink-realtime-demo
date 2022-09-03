package com.rison.bigdata.app.function;

import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.common.BigdataConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @PACKAGE_NAME: com.rison.bigdata.app.function
 * @NAME: DimSinkFunction
 * @USER: Rison
 * @DATE: 2022/9/3 23:36
 * @PROJECT_NAME: flink-realtime-demo
 **/
@Slf4j
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(BigdataConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(BigdataConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    /**
     * //value:{"sinkTable":"dim_base_trademark","database":"flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
     * //SQL：upsert into db.tn(id,tm_name) values('...','...')
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable,
                    after);
            log.info(upsertSql);
            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

//            //判断如果当前数据为更新操作,则先删除Redis中的数据
//            if ("update".equals(value.getString("type"))){
//                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
//            }

            //执行插入操作
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSql(String sinkTable, JSONObject after) {
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();

        //keySet.mkString(",");  =>  "id,tm_name"
        return "upsert into " + BigdataConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
