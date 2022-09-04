package com.rison.bigdata.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @PACKAGE_NAME: com.rison.bigdata.utils
 * @NAME: PhoenixJDBCUtil
 * @USER: Rison
 * @DATE: 2022/9/4 23:28
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class PhoenixJDBCUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clazz, boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){
            T t = clazz.newInstance();
            for (int i = 0; i < columnCount + 1; i ++){
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要装换为驼峰命名
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //获取列值
                Object object = resultSet.getObject(i);
                BeanUtils.setProperty(t, columnName, object);
            }
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }
}
