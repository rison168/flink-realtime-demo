package com.rison.bigdata.common;

/**
 * @PACKAGE_NAME: com.rison.bigdata.common
 * @NAME: BigdataConfig
 * @USER: Rison
 * @DATE: 2022/9/3 22:40
 * @PROJECT_NAME: flink-realtime-demo
 **/
public class BigdataConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL210325_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
