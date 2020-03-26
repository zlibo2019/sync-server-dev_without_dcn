package com.weds.database.syncserver.config;

/**
 * 对外使用的注册DCN服务的参数类
 */

public class RegisterProperties {

    // 表名
    private String tableName;

    // 字段名
    private String columnName;

    // 操作类型 insert/upate/delete/all
    private String operation;

    // 是否需要更改的详细信息
    private boolean isDetail;

    // 是否监听所有的字段
    private boolean isFullTable;

    private String routingKey;

    public RegisterProperties() {
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public boolean isDetail() {
        return isDetail;
    }

    public void setDetail(boolean detail) {
        isDetail = detail;
    }

    public boolean isFullTable() {
        return isFullTable;
    }

    public void setFullTable(boolean fullTable) {
        isFullTable = fullTable;
    }
}
