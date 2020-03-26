package com.weds.database.syncserver.cons;

public class CommonCons {

    // 注册监听配置表名
    public static final String REGISTER_TABLE_NAME = "cdc_register";

    public static final String ROUTER_PREFIX = "weds_";

    public static final String ORACLE_CDC_PREFIX = "CDC_";

    public static final String SQL_SERVER_CDC_PREFIX_TABLE = "dbo_";

    public static final String SQL_SERVER_CDC_SUFFIX_TABLE = "_CT";

    public static final String SQL_SERVER_CDC_PREFIX = "PK_";

    public static final String ROW_ID = "ROW_ID";

    public static final String ROW_NUM = "rnum";

    public static final String SERVER_NAME = "sync_server";

    public static final String CORE_COLUMN_TYPE = "_COLUMN_TYPE";

    // oracle cdc 用户名
    public static final String CDC_PUBLISHER = "CDC_PUBLISHER";

    // oracle cdc 变更集前缀
    public static final String CDC_CHANGE_SCOTT = "CDC_SCOTT_";

    // 要查询的Oracle数据库的字段
    public static final String CDC_OPERATION = "OPERATION$";

    public static final String CDC_COMMIT_TIMESTAMP = "COMMIT_TIMESTAMP$";

    public static final String CDC_ROW_ID = "ROW_ID$";

    public static final String CDC_TRAGET_COLMAP = "TARGET_COLMAP$";


    // 要查询的sql server数据库字段
    public static final String CDC_SQL_SERVER_OPERATION = "__$operation";

    public static final String CDC_SQL_SERVER_UPDATE_MASK = "__$update_mask";

    public static final String CDC_SQL_SERVER_TRAN_END_TIME = "tran_end_time";

}
