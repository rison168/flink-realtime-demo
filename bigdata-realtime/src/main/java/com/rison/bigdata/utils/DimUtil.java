package com.rison.bigdata.utils;

import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.common.BigdataConfig;
import org.apache.phoenix.util.JDBCUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @PACKAGE_NAME: com.rison.bigdata.utils
 * @NAME: DimUtil
 * @USER: Rison
 * @DATE: 2022/9/4 21:56
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //拼接查询语句
        String querySql = "SELECT * FROM " + BigdataConfig.HBASE_SCHEMA + "." +
                tableName + " WHERE id='" + id + "'";

        //查询Phoenix
        List<JSONObject> jsonObjects = PhoenixJDBCUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject jsonObject = jsonObjects.get(0);

        return jsonObject;

    }

}
