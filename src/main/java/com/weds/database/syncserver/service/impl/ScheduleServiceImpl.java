package com.weds.database.syncserver.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weds.database.syncserver.config.RabbitmqProperties;
import com.weds.database.syncserver.config.ScheduleProperties;
import com.weds.database.syncserver.config.SyncDBProperties;
import com.weds.database.syncserver.cons.CommonCons;
import com.weds.database.syncserver.service.CoreDataSync;
import com.weds.database.syncserver.service.ScheduleService;
import com.weds.database.syncserver.utils.ConnectionFactory;
import com.weds.database.syncserver.utils.InstantUtil;
import com.weds.framework.mq.RabbitmqService;
import org.apache.log4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@Transactional
public class ScheduleServiceImpl implements ScheduleService {

    @Resource
    private ScheduleProperties scheduleProperties;

    @Resource
    private TaskScheduler taskScheduler;

    @Resource
    private ScheduledExecutorService scheduledExecutorService;

    @Resource
    private ConnectionFactory connectionFactory;

    @Resource
    private SyncDBProperties syncDBProperties;

    @Resource
    private RabbitmqProperties rabbitmqProperties;

    @Resource
    private RabbitmqService rabbitmqService;

    @Resource(name = "oracleJdbcTemplate")
    private JdbcTemplate oracleJdbcTemplate;

    @Resource(name = "pgJdbcTemplate")
    private JdbcTemplate pgJdbcTemplate;

    @Resource(name = "sqlServerJdbcTemplate")
    private JdbcTemplate sqlServerJdbcTemplate;

    @Resource
    private CoreDataSync coreDataSync;

    @Resource
    private InstantUtil instantUtil;

    private List<String> coreTables;

    private Map<String, Object> columnMap;

    private Map<String, Object> configMap;

    private Map<String, List<String>> targetTables;

    private List<String> targetTableNameList;

    private List<String> coreTableNameList;

    private Logger log = Logger.getLogger(getClass());

    // 查询的时间条件集合
    private Map<String, Object> conditionTimes= new HashMap<>();

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private RabbitTemplate rabbitTemplate = null;

    private CopyOnWriteArrayList<Object> receiveMsgs = new CopyOnWriteArrayList<>();

    private boolean syncLock = false;

    private Map<String, List<String>> coreTablesColumns = new HashMap<>();

    private Statement tarStatement;

    private SimpleMessageListenerContainer container;

    private ScheduledFuture<?> future = null;

    /**
     * 计划任务
     * 监听数据库数据改变
     *
     * @return
     */
    @Override
    public void listenDbChanges() throws Exception {
        // 首先开启对表的监听功能
        // 读取配置表信息
        configMap = coreDataSync.getCdcConfig();
        // 获取要同步的表名称
        coreTables = (List<String>) configMap.get("coreTableNameList");
        // 核心表字段对应的目标表字段
        columnMap = (Map<String, Object>) configMap.get("columnMap");
        // 目标表
        targetTables = (Map<String, List<String>>) configMap.get("targetTables");
        // 目标表的名称
        targetTableNameList = (List<String>) configMap.get("targetTableNameList");
        coreTableNameList = (List<String>) configMap.get("coreTableNameList");
        // 开启对表的监听
        switch (syncDBProperties.getCoreDbType()) {
            case 1: // oracle
                startOracleCDC();
                // 获取到所有的表对应的时间条件
                for (int i = 0; i < coreTableNameList.size(); i++) {
                    String coreTableName = coreTableNameList.get(i);
                    Map<String, Object> oracleMap = pgJdbcTemplate.queryForMap("select condition_time from cdc_update_timestamp where flag = ? and core_table_name = ?", "oracle", coreTableName);
                    conditionTimes.put(coreTableName, oracleMap.get("condition_time"));
                }
                // 获取数据库连接
                Connection coreConnection = connectionFactory.getInitConnection(2);
                // 执行计划任务，定期查询
                future = scheduledExecutorService.scheduleAtFixedRate(() -> {
                    getChangeDataFromOracle(coreConnection);
                }, 0, scheduleProperties.getFixedRate(), TimeUnit.SECONDS);
                break;
            case 2: // sql server
                startSqlServerCDC();
                // 获取到所有的表对应的时间条件
                for (int i = 0; i < coreTableNameList.size(); i++) {
                    String coreTableName = coreTableNameList.get(i);
                    Map<String, Object> sqlServerMap = pgJdbcTemplate.queryForMap("select condition_time from cdc_update_timestamp where flag = ? and core_table_name = ?", "sql_server", coreTableName);
                    conditionTimes.put(coreTableName, sqlServerMap.get("condition_time"));
                }
                // 执行计划任务，定期查询
                future = scheduledExecutorService.scheduleAtFixedRate(this::getChangeDataFromSqlServer, 0, scheduleProperties.getFixedRate(), TimeUnit.SECONDS);
                break;
            case 3:
                // mysql 只需要注册监听rabbitmq即可
                log.info("begin listening rabbitmq");
                Connection targetConn = connectionFactory.getInitConnection(1);
                // 获取当前的时间
                instantUtil.setInstan(Instant.now());
                coreTablesColumns = (Map<String, List<String>>) configMap.get("coreTables");
                targetConn.setAutoCommit(false);// 设置手动提交
                tarStatement = targetConn.createStatement();
                rabbitmqProperties.setExchangeType("fanout");
                final int commitDuration = rabbitmqProperties.getCommitDuration();
                container = rabbitmqService.subscribeTopic(rabbitmqProperties, (message) -> {
                    try {
                        // 获取接收到消息的时间
                        Instant insReceive = Instant.now();
                        // 把after赋值给before
                        instantUtil.setInstan(insReceive);
                        while (receiveMsgs.size() == (50000) && !syncLock) {
                            // 当内存中存入了预取的数据量后，让程序先执行这些数据
                            mysqlSync(targetConn);
                        }
                        while (syncLock) {
                            Thread.sleep(commitDuration * 1000);
                        }
                        receiveMsgs.add(message);
                        log.info("add success");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });

                // 执行计划任务，定期查询
                taskScheduler.scheduleAtFixedRate(doMysqlSync(targetConn), scheduleProperties.getFixedRate());
                break;
            default:
                break;
        }
    }

    private Runnable doMysqlSync(Connection targetConn) {
        return () -> {
            int commitDuration = rabbitmqProperties.getCommitDuration();
            // 获取接收到消息的时间
            Instant insReceive = Instant.now();
            // 计算接收时间差
            Duration duration = Duration.between(instantUtil.getInstan(), insReceive);
            // 时间差，秒为单位
            long durationSec = duration.getSeconds();
            if (!syncLock && durationSec >= commitDuration) {

                log.info("定是计划：开始同步");
                mysqlSync(targetConn);
            }
        };
    }


    private synchronized void mysqlSync(Connection targetConn) {
        try {
            syncLock = true;
            if (syncDBProperties.getCoreDbType() != 3 || receiveMsgs.size() <=0) {
                // 如果不是对mysql数据库进行同步，不需要进行下面的操作。
                // 如果消息集合中没有获取到消息，直接退出
                syncLock = false;
                return;
            }

            final List<Map<String, Object>> changeList = new ArrayList<>();
            for (int i1 = 0; i1 < coreTables.size(); i1++) {
                String coreTable = coreTables.get(i1);
                List<String> columns = coreTablesColumns.get(coreTable);
                String targetTableName = targetTableNameList.get(i1);
                List<String> targetColumnsUsed = targetTables.get(targetTableName);
                String identifyTargetColumn = targetColumnsUsed.get(0);
                String identifyCoreColumn = columns.get(0);

                String sqlCondition = "SELECT * FROM " + targetTableName + " LIMIT 0";
                ResultSet conditionRs = tarStatement.executeQuery(sqlCondition);
                ResultSetMetaData conditionRsData = conditionRs.getMetaData();

                // insert statement
                StringBuffer columnsStr = new StringBuffer();
                StringBuffer bindVariables = new StringBuffer();
                for (int j = 0; j < targetColumnsUsed.size(); j++) {
                    columnsStr.append(targetColumnsUsed.get(j));
                    columnsStr.append(",");
                    bindVariables.append('?');
                    bindVariables.append(",");
                }
                String columnsStrFinal = columnsStr.substring(0, columnsStr.length() - 1);
                String bindVariablesFinal = bindVariables.substring(0, bindVariables.length() - 1);
                bindVariables.substring(0, bindVariables.length() - 1);
                String insertSql = "INSERT INTO " + targetTableName + " ("
                        + columnsStrFinal
                        + ") VALUES ("
                        + bindVariablesFinal
                        + ")";
                List<Integer> typeList = new ArrayList<>();
                for (int i = 0; i < conditionRsData.getColumnCount(); i++) {
                    int typeTemp = conditionRsData.getColumnType(i + 1);
                    typeList.add(typeTemp);
                }
                PreparedStatement targetInsertPstmt = targetConn.prepareStatement(insertSql);

                // update statement
                StringBuilder targetUpdateColumns = new StringBuilder();
                StringBuilder columnUpdate = new StringBuilder();
                List<String> placeholder = new ArrayList<>();
                List<Integer> columnUpdateType = new ArrayList<>();
                for (int i = 0; i < targetColumnsUsed.size(); i++) {
                    String columnName = columns.get(i);
                    String targetColumnName = targetColumnsUsed.get(i);
                    int updateColumnType = conditionRsData.getColumnType(i + 1);
                    columnUpdate.append(targetColumnName);
                    columnUpdate.append(" = ?");
                    columnUpdate.append(",");
                    targetUpdateColumns.append(targetColumnName);
                    targetUpdateColumns.append(",");
                    placeholder.add(columnName);
                    columnUpdateType.add(updateColumnType);
                }
                String columnUpdateStr = columnUpdate.substring(0, columnUpdate.length() - 1);
                String targetColumnsFinal = targetUpdateColumns.substring(0, targetUpdateColumns.length() - 1);
                String updateSql = "UPDATE " + targetTableName + " SET " + columnUpdateStr + " WHERE " + identifyTargetColumn + "= ?";
                PreparedStatement targetUpdatePstmtMysql = targetConn.prepareStatement(updateSql);

                // delete statement
                String delSql = "DELETE FROM " + targetTableName + " WHERE " + identifyTargetColumn + "= ?";
                PreparedStatement targetDelPstmt = targetConn.prepareStatement(delSql);
                receiveMsgs.stream().map(str -> JSON.parseObject(new String((byte[]) str)))
                        .filter(json -> json.get("table").toString().equals(coreTable))
                        .forEach(receivedMsg -> {
                            try {
                                // 首先判断数据库是否一致
                                if (!rabbitmqProperties.getDatabase().trim().equals(receivedMsg.get("database"))) {
                                    return;
                                }
                                // 判断表是否是监听表
                                if (!coreTables.contains(receivedMsg.get("table").toString())) {
                                    return;
                                }
                                // 判断是否有监听的字段的标识
                                int i3 = -1;
                                JSONObject old = (JSONObject) receivedMsg.get("old");
                                if (null == old) {
                                    i3 = 1;
                                } else {
                                    for (int i = 0; i < columns.size(); i++) {
                                        String columnName = columns.get(i);
                                        if (old.containsKey(columnName)) {
                                            i3 = i;
                                        }
                                    }
                                }
                                if (-1 == i3) {
                                    // 配置表中没有对这个表的监听，那么不需要进行数据同步
                                    return;
                                }

                                log.info("received msg:" + receivedMsg);
                                JSONObject data = (JSONObject) receivedMsg.get("data");
                                // 进行数据同步
                                String type = receivedMsg.get("type").toString();
                                JSONObject changeData = new JSONObject();

                                switch (type) {
                                    case "insert":
                                        log.info(insertSql);
                                        for (int i = 0; i < columns.size(); i++) {
                                            String columnName = columns.get(i);
                                            Object columnValue = data.get(columnName);
                                            int insertTypeTemp = typeList.get(i);
                                            if (insertTypeTemp == Types.TIMESTAMP) {
                                                // 如果是时间戳类型，由于maxwell默认使用utc格式的区时，转换为cst需要增加8个小时
                                                Timestamp utcInsertTimeStamp = Timestamp.valueOf(columnValue.toString());
                                                utcInsertTimeStamp.setTime(utcInsertTimeStamp.getTime() + ((8 * 60) * 60) * 1000);
                                                targetInsertPstmt.setTimestamp((i + 1), utcInsertTimeStamp);
                                                changeData.put(columnName, sdf.format(utcInsertTimeStamp));
                                            } else {
                                                targetInsertPstmt.setObject(i + 1, columnValue, insertTypeTemp);
                                                changeData.put(columnName, columnValue);
                                            }
                                        }
                                        targetInsertPstmt.addBatch();
                                        setMysqlMessage(type, identifyTargetColumn, receivedMsg.get("ts").toString(), targetTableName, columnsStrFinal, changeData, changeList);
                                        break;
                                    case "update":
                                        log.info(updateSql);
                                        for (int i = 0; i < placeholder.size(); i++) {
                                            int updateTypeTemp = columnUpdateType.get(i);
                                            if (updateTypeTemp == Types.TIMESTAMP) {
                                                // 如果是时间戳类型，由于maxwell默认使用utc格式的区时，转换为cst需要增加8个小时
                                                String utcUpdateTime = (String) data.get(placeholder.get(i));
                                                Timestamp utcUpdateTimeStamp = Timestamp.valueOf(utcUpdateTime);
                                                utcUpdateTimeStamp.setTime(utcUpdateTimeStamp.getTime() + ((8 * 60) * 60) * 1000);
                                                targetUpdatePstmtMysql.setTimestamp((i + 1), utcUpdateTimeStamp);
                                                changeData.put(placeholder.get(i), sdf.format(utcUpdateTimeStamp));
                                            } else {
                                                targetUpdatePstmtMysql.setObject((i + 1), data.get(placeholder.get(i)), updateTypeTemp);
                                                changeData.put(placeholder.get(i), data.get(placeholder.get(i)));
                                            }
                                        }
                                        log.info("placeholder:" + (placeholder.size() + 1) + ",data:" + data.get(identifyCoreColumn));
                                        targetUpdatePstmtMysql.setObject((placeholder.size() + 1), data.get(identifyCoreColumn));
                                        targetUpdatePstmtMysql.addBatch();
                                        setMysqlMessage(type, identifyTargetColumn, receivedMsg.get("ts").toString(), targetTableName, targetColumnsFinal, changeData, changeList);
                                        break;
                                    case "delete":
                                        log.info(delSql);
                                        targetDelPstmt.setObject(1, data.get(identifyCoreColumn));
                                        log.info("placeholder:" + 1 + ",data:" + data.get(identifyCoreColumn));
                                        targetDelPstmt.addBatch();
                                        setMysqlMessage(type, identifyTargetColumn, receivedMsg.get("ts").toString(), targetTableName, targetColumnsFinal, null, changeList);
                                        break;
                                    default:
                                        break;
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                        });
                // 提交
                targetInsertPstmt.executeBatch();
                targetInsertPstmt.clearBatch();
                targetUpdatePstmtMysql.executeBatch();
                targetUpdatePstmtMysql.clearBatch();
                targetDelPstmt.executeBatch();
                targetDelPstmt.clearBatch();
                targetConn.commit();
                log.info("commit:" + coreTable);
            }
            sentMessage(changeList);
            receiveMsgs.clear();
            syncLock = false;
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }


    /**
     * 关闭服务前确保内存中的所有数据都已经处理了。
     * @throws Exception
     */
    @PreDestroy
    public void clearMovieCache() throws Exception {
        if (receiveMsgs.size() > 0) {
            container.shutdown();
            Connection connection = connectionFactory.getInitConnection(1);
            connection.setAutoCommit(false);
            mysqlSync(connection);
        }
    }

    private void getChangeDataFromSqlServer() {
        try {
            // 获取数据库连接
            Connection coreConnection = connectionFactory.getInitConnection(4);
            Statement stmt = coreConnection.createStatement();
            List<String> coreUser = (List<String>) configMap.get("coreUser");
            for (int i1 = 0; i1 < coreTables.size(); i1++) {
                String tableName = coreTables.get(i1);
                log.info("定时任务，同步表：" + tableName);
                String conditionTime = conditionTimes.get(tableName).toString();
                List<String> targetColumnsUsed = targetTables.get(targetTableNameList.get(i1));
                String targetTableName = targetTableNameList.get(i1);
                // 拼接sql
                Map<String, List<String>> tableInfo = (Map<String, List<String>>) configMap.get("coreTables");
                List<String> coreColumns = tableInfo.get(tableName);
                // 获取表的前缀
                String coreUserName = coreUser.get(i1);
                String suffixName = coreUserName.split("\\.")[1];
                String prefixName = coreUserName.split("\\.")[0];
                String coreUserNameNew = prefixName + ".cdc";
                StringBuffer coreOtherColumns = new StringBuffer();
                for (String coreColumn : coreColumns) {
                    coreOtherColumns.append(coreColumn);
                    coreOtherColumns.append(",");
                }
                String coreColumnStr = coreOtherColumns.substring(0, coreOtherColumns.length() - 1);
                String coreSql = "select a." + CommonCons.CDC_SQL_SERVER_OPERATION + ", a." + CommonCons.CDC_SQL_SERVER_UPDATE_MASK + ",b." +
                        CommonCons.CDC_SQL_SERVER_TRAN_END_TIME + "," + coreColumnStr +
                        " from " + coreUserNameNew + "." + suffixName + "_" + tableName + CommonCons.SQL_SERVER_CDC_SUFFIX_TABLE + " a LEFT JOIN " + coreUserNameNew +".lsn_time_mapping b on a.__$start_lsn = b.start_lsn where b." + CommonCons.CDC_SQL_SERVER_TRAN_END_TIME + " > '" + conditionTime + "' ORDER BY "+ CommonCons.CDC_SQL_SERVER_TRAN_END_TIME + " ASC";
                ResultSet rs = stmt.executeQuery(coreSql);
                log.info("查询的时间条件是:" + conditionTime);
                Timestamp timestamp;
                if (rs.next()) {
                    log.info("开始同步数据，关闭定时任务...");
                    future.cancel(true);
                    long batchCounter = 0; //累加的批处理数量
                    timestamp = new Timestamp(sdf.parse(conditionTime).getTime());
                    Instant instBegin = Instant.now();
                    // 重置rs.next
                    rs = stmt.executeQuery(coreSql);
                    // 获取元数据
                    ResultSetMetaData meta = rs.getMetaData();
                    List<Map<String, Object>> changeList = new ArrayList<>();
                    // 获得目标库连接
                    Connection targetConn = connectionFactory.getInitConnection(1);
                    targetConn.setAutoCommit(false);// 设置手动提交


                    // 声明List存放columns
                    StringBuilder columnNames = new StringBuilder();
                    StringBuilder bindVariables = new StringBuilder();
                    String identifyColumn = "";
                    // update 用的字段名称和占位
                    StringBuilder updateColumns = new StringBuilder();
                    // 获取真正的源表字段
                    for (int i = 0; i < targetColumnsUsed.size(); i++) {
                        // 第一个字段就是主键
                        if (i == 0) {
                            identifyColumn = targetColumnsUsed.get(i);
                        }

                        columnNames.append(targetColumnsUsed.get(i));
                        columnNames.append(", ");
                        bindVariables.append('?');
                        bindVariables.append(", ");
                        updateColumns.append(targetColumnsUsed.get(i) + " = ?");
                        updateColumns.append(",");
                    }
                    columnNames = new StringBuilder(columnNames.substring(0, columnNames.length() - 2));
                    bindVariables = new StringBuilder(bindVariables.substring(0, bindVariables.length() - 2));
                    String insertSql = "INSERT INTO " + targetTableName + " ("
                            + columnNames
                            + ") VALUES ("
                            + bindVariables
                            + ")";
                    PreparedStatement targetInsertPstmt = targetConn.prepareStatement(insertSql);

                    // 更新
                    String updateColumnsStr = updateColumns.substring(0, updateColumns.length() - 1);
                    String updateSql = "UPDATE " + targetTableName + " SET " +updateColumnsStr + " WHERE " + identifyColumn + "= ?";
                    PreparedStatement targetUpdatePstmtSqlServer = targetConn.prepareStatement(updateSql);

                    // 删除
                    String delSql = "DELETE FROM " + targetTableName + " WHERE " + identifyColumn + "= ?";
                    PreparedStatement targetDelPstmt = targetConn.prepareStatement(delSql);
                    while (rs.next()) {
                        log.info(tableName + "发生数据变化，开始同步");
                        // 获取操作类型
                        String operationType = String.valueOf(rs.getObject(1)).trim();
                        String operation = "";
                        timestamp = rs.getTimestamp(3);
                        conditionTime = sdf.format(timestamp);
                        conditionTimes.put(tableName, conditionTime);
                        // 获取修改的字段的16进制字符串
                        String hexChanges = rs.getString(2);
                        String binaryStr;
                        switch (operationType) {
                            case "2": //insert
                                log.info("insert");
                                operation = "insert";
                                binaryStr = getSubHex4SqlServer(hexChanges, coreColumns);
                                // 设置占位的?对应的值以及数据类型
                                targetInsertPstmt = setSqlServerValue(targetInsertPstmt, meta, rs);
                                targetInsertPstmt.addBatch();
                                // 设置修改信息集(以字段为单位)
                                changeList = setSqlServerMessage(binaryStr, tableName, identifyColumn, operation, rs, changeList, coreColumns);
                                batchCounter++;
                                break;
                            case "4": // update
                                log.info("update");
                                operation = "update";
                                // 2进制字符串
                                binaryStr = getSubHex4SqlServer(hexChanges, coreColumns);
                                for (int i = 0; i < targetColumnsUsed.size(); i++) {
                                    targetUpdatePstmtSqlServer.setObject(i + 1, rs.getObject(i + 1 + 3));
                                }
                                targetUpdatePstmtSqlServer.setObject(targetColumnsUsed.size() + 1, rs.getObject(4));
                                targetUpdatePstmtSqlServer.addBatch();
                                // 设置修改信息集(以字段为单位)
                                changeList = setSqlServerMessage(binaryStr, tableName, identifyColumn, operation, rs, changeList, coreColumns);
                                batchCounter++;
                                break;
                            case "1": // delete
                                // 2进制字符串
                                log.info("delete");
                                operation = "delete";
                                targetDelPstmt.setObject(1, rs.getObject(4));
                                targetDelPstmt.addBatch();
                                // 设置修改信息集(以字段为单位)
                                changeList = setSqlServerMessage("", tableName, identifyColumn, operation, rs, changeList, coreColumns);
                                batchCounter++;
                                break;
                            default:
                                // 其他所有的情况不需要进行任何操作。
                                break;

                        }
                        if (batchCounter % 50000 == 0) { //1万条数据一提交
                            log.info("commit" + tableName);
                            targetDelPstmt.executeBatch();
                            targetDelPstmt.clearBatch();
                            targetUpdatePstmtSqlServer.executeBatch();
                            targetUpdatePstmtSqlServer.clearBatch();
                            targetInsertPstmt.executeBatch();
                            targetInsertPstmt.clearBatch();
                            targetConn.commit();
                        }
                    }
                    // 批量提交
                    log.info("commit" + tableName);
                    targetInsertPstmt.executeBatch();
                    targetInsertPstmt.clearBatch();
                    targetUpdatePstmtSqlServer.executeBatch();
                    targetUpdatePstmtSqlServer.clearBatch();
                    targetDelPstmt.executeBatch();
                    targetDelPstmt.clearBatch();
                    targetConn.commit();
                    // 释放所有连接
                    connectionFactory.release(targetConn, targetInsertPstmt, targetDelPstmt);
                    rs.close();
                    Instant instEnd = Instant.now();
                    log.info("同步耗时:" + Duration.between(instBegin, instEnd).toMillis());
                    // 更改时间
                    final Timestamp dateParam =timestamp;
                    pgJdbcTemplate.update("UPDATE cdc_update_timestamp set condition_time = ? where flag = ? and core_table_name = ?",(PreparedStatement ps) -> {
                        ps.setTimestamp(1, dateParam);
                        ps.setObject(2, "sql_server");
                        ps.setObject(3, tableName);
                    });
//                        pgJdbcTemplate.getDataSource().getConnection().commit();
                    // rabbitmq推送所有的数据改变
                    sentMessage(changeList);
                    future = scheduledExecutorService.scheduleAtFixedRate(this::getChangeDataFromSqlServer, 0, scheduleProperties.getFixedRate(), TimeUnit.SECONDS);
                }
            }
            connectionFactory.release(coreConnection, stmt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getChangeDataFromOracle(Connection coreConnection) {
        try {
            Statement stmt = coreConnection.createStatement();
            for (int i1 = 0; i1 < coreTables.size(); i1++) {
                String tableName = coreTables.get(i1);
                log.info("定时任务，同步表：" + tableName);
                String conditionTime = conditionTimes.get(tableName).toString();
                List<String> targetColumnsUsed = targetTables.get(targetTableNameList.get(i1));
                String targetTableName = targetTableNameList.get(i1);
                // 查询数据库中字段对应的序号
                String columnsSql = "SELECT COLUMN_NAME, COLUMN_ID FROM ALL_TAB_COLUMNS WHERE OWNER=? and TABLE_NAME=? order by COLUMN_ID";
                // 获取到字段对应的下标的集合
                List<Map<String, Object>> columnMapList = oracleJdbcTemplate.queryForList(columnsSql, CommonCons.CDC_PUBLISHER, CommonCons.ORACLE_CDC_PREFIX + tableName);
                // 字段和下表的map对应集
                Map<String, Object> columnIndexMap = new LinkedHashMap<>();
                for (Map<String, Object> mapEntity : columnMapList) {
                    columnIndexMap.put(String.valueOf(mapEntity.get("COLUMN_ID")), mapEntity.get("COLUMN_NAME"));
                }
                // 拼接sql
                Map<String, List<String>> tableInfo = (Map<String, List<String>>) configMap.get("coreTables");
                List<String> coreColumns = tableInfo.get(tableName);
                StringBuffer coreOtherColumns = new StringBuffer();
                for (String coreColumn : coreColumns) {
                    coreOtherColumns.append(coreColumn);
                    coreOtherColumns.append(",");
                }
                String coreColumnStr = coreOtherColumns.substring(0, coreOtherColumns.length() - 1);
                String coreSql = "select " + CommonCons.CDC_OPERATION + "," + CommonCons.CDC_COMMIT_TIMESTAMP + "," +
                        CommonCons.CDC_ROW_ID + "," + CommonCons.CDC_TRAGET_COLMAP + "," + coreColumnStr +
                        " from " + CommonCons.ORACLE_CDC_PREFIX + tableName + " where COMMIT_TIMESTAMP$ > TIMESTAMP '" + conditionTime + "'";
                ResultSet rs = stmt.executeQuery(coreSql);
                Timestamp timestamp;
                if (rs.next()) {
                    log.info("开始同步数据，关闭定时任务...");
                    future.cancel(true);
                    long batchCounter = 0; //累加的批处理数量
                    timestamp = new Timestamp(sdf.parse(conditionTime).getTime());
                    Instant instBegin = Instant.now();
                    // 重置rs.next
                    rs = stmt.executeQuery(coreSql);
                    // 获取元数据
                    ResultSetMetaData meta = rs.getMetaData();
                    List<Map<String, Object>> changeList = new ArrayList<>();
                    // 获得目标库连接
                    Connection targetConn = connectionFactory.getInitConnection(1);
                    targetConn.setAutoCommit(false);// 设置手动提交

                    // insert 用的字段占位和名称
                    StringBuilder columnNames = new StringBuilder();
                    StringBuilder bindVariables = new StringBuilder();
                    // update 用的字段名称和占位
                    StringBuilder updateColumns = new StringBuilder();
                    // 获取真正的源表字段
                    for (int i = 0; i < targetColumnsUsed.size(); i++) {
                        columnNames.append(targetColumnsUsed.get(i));
                        columnNames.append(", ");
                        bindVariables.append('?');
                        bindVariables.append(", ");
                        updateColumns.append(targetColumnsUsed.get(i) + "= case when ? then " + targetColumnsUsed.get(i) +" else ? end");
                        updateColumns.append(",");
                    }
                    // 添加rowID字段
                    columnNames.append(CommonCons.ROW_ID);
                    bindVariables.append('?');
                    String insertSql = "INSERT INTO " + targetTableName + " ("
                            + columnNames
                            + ") VALUES ("
                            + bindVariables
                            + ")";
                    PreparedStatement targetInsertPstmt = targetConn.prepareStatement(insertSql);

                    // 更新
                    String updateColumnsStr = updateColumns.substring(0, updateColumns.length() - 1);
                    String updateSql = "UPDATE " + targetTableName + " SET " +updateColumnsStr + " WHERE " + CommonCons.ROW_ID + "= ?";
                    PreparedStatement targetUpdatePstmtOracle = targetConn.prepareStatement(updateSql);

                    // 删除
                    String delSql = "DELETE FROM " + targetTableName + " WHERE " + CommonCons.ROW_ID + "= ?";
                    PreparedStatement targetDelPstmt = targetConn.prepareStatement(delSql);
                    while (rs.next()) {
                        log.info(tableName + "发生数据变化，开始同步");

                        // 获取操作类型
                        String operationType = String.valueOf(rs.getObject(1)).trim();
                        // 获取修改的字段的16进制字符串
                        String hexChanges = rs.getString(4);
                        String binaryStr;
                        // 更新条件中的时间戳
                        timestamp = rs.getTimestamp(2);
                        conditionTime = sdf.format(timestamp);
                        conditionTimes.put(tableName, conditionTime);
                        String operation = "";
                        switch (operationType) {
                            case "I": //insert
                                log.info("insert");
                                operation = "insert";
                                // 2进制字符串
                                binaryStr = getSubHex(hexChanges, coreColumns, columnIndexMap);

                                // 设置占位的?对应的值以及数据类型
                                targetInsertPstmt = setValue(targetInsertPstmt, meta, rs);
                                targetInsertPstmt.addBatch();
                                // 设置修改信息集(以字段为单位)
                                changeList = setMessage(binaryStr, tableName, operation, rs, changeList, coreColumns);
                                batchCounter++;
                                break;
                            case "UN": // update
                                log.info("update");
                                operation = "update";
                                // 2进制字符串
                                binaryStr = getSubHex(hexChanges, coreColumns, columnIndexMap);

                                if (!"".equals(binaryStr)) {
                                    // 把binaryStr 后面缺少的字段补上0
                                    if (targetColumnsUsed.size() > binaryStr.length()) {
                                        int binaryLength = binaryStr.length();
                                        for (int i = 0; i < targetColumnsUsed.size() - binaryLength; i++) {
                                            binaryStr = binaryStr + "0";
                                        }
                                    }
                                    // 第八个往后的column是数据表对应的column
                                    for (int i = 0; i < binaryStr.length(); i++) {
                                        String index = binaryStr.substring(i, i + 1);
                                        if ("1".equals(index)) {
                                            targetUpdatePstmtOracle.setObject((i + 1) * 2 - 1, false, Types.BOOLEAN);
                                            targetUpdatePstmtOracle.setObject((i + 1) * 2, rs.getObject(i + 1 + 4), meta.getColumnType(i + 1 + 4));
                                        } else {
                                            targetUpdatePstmtOracle.setObject((i + 1) * 2 - 1, true, Types.BOOLEAN);
                                            targetUpdatePstmtOracle.setObject((i + 1) * 2, rs.getObject(i + 1 + 4), meta.getColumnType(i + 1 + 4));
                                        }
                                    }
                                    targetUpdatePstmtOracle.setObject((targetColumnsUsed.size() * 2) + 1, rs.getString(3), Types.VARCHAR);
                                    targetUpdatePstmtOracle.addBatch();
                                    // 设置修改信息集(以字段为单位)
                                    changeList = setMessage(binaryStr, tableName, operation, rs, changeList, coreColumns);
                                    batchCounter++;
                                }
                                break;
                            case "D":
                                // 2进制字符串
                                log.info("delete");
                                operation = "delete";
                                targetDelPstmt.setObject(1, rs.getString(3), Types.VARCHAR);
                                targetDelPstmt.addBatch();
                                System.out.println(rs.getString(7));
                                // 设置修改信息集(以字段为单位)
                                changeList = setMessage("", tableName, operation, rs, changeList, coreColumns);
                                batchCounter++;
                                break;
                            default:
                                // 其他所有的情况不需要进行任何操作。
                                break;

                        }
                        if (batchCounter % 50000 == 0) { //1万条数据一提交
                            log.info("commit" + tableName);
                            targetInsertPstmt.executeBatch();
                            targetInsertPstmt.clearBatch();
                            targetUpdatePstmtOracle.executeBatch();
                            targetUpdatePstmtOracle.clearBatch();
                            targetDelPstmt.executeBatch();
                            targetDelPstmt.clearBatch();
                            targetConn.commit();
                        }

                    }
                    // 批量提交
                    log.info("commit" + tableName);
                    targetInsertPstmt.executeBatch();
                    targetInsertPstmt.clearBatch();
                    targetUpdatePstmtOracle.executeBatch();
                    targetUpdatePstmtOracle.clearBatch();
                    targetDelPstmt.executeBatch();
                    targetDelPstmt.clearBatch();
                    targetConn.commit();
                    // 释放所有连接
//                        connectionFactory.release(targetConn, targetInsertPstmt, targetDelPstmt);
                    rs.close();
                    Instant instEnd = Instant.now();
                    log.info("同步耗时：" + Duration.between(instBegin, instEnd).toMillis());

                    // 更改时间
                    final Timestamp dateParam =timestamp;
                    pgJdbcTemplate.update("UPDATE cdc_update_timestamp set condition_time = ? where flag = ? and core_table_name = ?",(PreparedStatement ps) -> {
                        ps.setTimestamp(1, dateParam);
                        ps.setObject(2, "oracle");
                        ps.setObject(3, tableName);
                    });
                    // rabbitmq推送所有的数据改变
                    sentMessage(changeList);
                    future = scheduledExecutorService.scheduleAtFixedRate(() -> {
                        getChangeDataFromOracle(coreConnection);
                    }, 0, scheduleProperties.getFixedRate(), TimeUnit.SECONDS);
                }
            }
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行存储过程，开启oracle的
     * @return
     */
    @SuppressWarnings({"Duplicates", "unchecked"})
    private int startOracleCDC() throws Exception {
        List<String> coreUser = (List<String>) configMap.get("coreUser");
        List<String> coreTableNameList = (List<String>) configMap.get("coreTableNameList");
        Map<String, List<String>> coreTables = (Map<String, List<String>>) configMap.get("coreTables");
        // 遍历要监听的表
        for (int i = 0; i < coreTableNameList.size(); i++) {
            String tableNameUsed = coreTableNameList.get(i);
            String tableUserUsed = coreUser.get(i);
            String sql1 = "{call sp_cdc_capture_config(?)}";
            String sql3 = "{call sp_cdc_capture_config_table(?,?,?)}";
            // 拼装sql3的第三个参数
            List<String> columns = coreTables.get(coreTableNameList.get(i));
            List<String> columnTypes = coreTables.get(coreTableNameList.get(i) + CommonCons.CORE_COLUMN_TYPE);
            StringBuilder param3 = new StringBuilder();
            for (int j = 0; j < columns.size(); j++) {
                param3.append(columns.get(j));
                param3.append(" ");
                param3.append(columnTypes.get(j));
                if (j != columns.size() -1) {
                    param3.append(",");
                }
            }

            // 执行第三个存储过程(创建变更表)前确认是否有变更表
            String confirmSql3 = "select * from change_tables where change_table_name = ?";
            List<Map<String, Object>> confrimMap3 = oracleJdbcTemplate.queryForList(confirmSql3, CommonCons.ORACLE_CDC_PREFIX + tableNameUsed);
            if (confrimMap3.size() <= 0) {
                oracleJdbcTemplate.execute(sql1, (PreparedStatement ps) -> {
                    ps.setString(1, tableUserUsed + "." + tableNameUsed);
                    ps.execute();
                    return null;
                });
                oracleJdbcTemplate.execute(sql3, (PreparedStatement ps) -> {
                    ps.setString(1, tableUserUsed);
                    ps.setString(2, tableNameUsed);
                    ps.setString(3, String.valueOf(param3).toLowerCase());
                    ps.execute();
                    return null;
                });
            }
        }
        return 0;
    }

    private void startSqlServerCDC() {
        List<String> coreUser = (List<String>) configMap.get("coreUser");
        List<String> coreTableNameList = (List<String>) configMap.get("coreTableNameList");
        Map<String, List<String>> coreTables = (Map<String, List<String>>) configMap.get("coreTables");
        // 遍历要监听的表
        for (int i = 0; i < coreTableNameList.size(); i++) {
            String tableNameUsed = coreTableNameList.get(i);
            String tableUserUsed = coreUser.get(i);
            // 拼装sql3的第三个参数
            List<String> columns = coreTables.get(coreTableNameList.get(i));
            StringBuilder param3 = new StringBuilder();
            for (int j = 0; j < columns.size(); j++) {
                param3.append(columns.get(j));
                if (j != columns.size() -1) {
                    param3.append(",");
                }
            }
            String sql = "exec open_cdc_table @table_name=?,@source_schema_in='dbo',@captured_column_list_in=?";
            String coreUserName = coreUser.get(i).replace("dbo", "cdc");
            String sqlConfirm = "select * from " + coreUserName + ".change_tables where index_name = ?";
            List<Map<String, Object>> confrimMap = sqlServerJdbcTemplate.queryForList(sqlConfirm, CommonCons.SQL_SERVER_CDC_PREFIX + tableNameUsed);
            if (confrimMap.size() <= 0) {
                sqlServerJdbcTemplate.execute(sql, (PreparedStatement ps) -> {
                    ps.setString(1, tableNameUsed);
                    ps.setString(2, param3.toString());
                    ps.execute();
                    return null;
                });
            }
        }
    }

    /**
     * 设置值
     * @param stmt
     * @return
     */
    private PreparedStatement setValue(PreparedStatement stmt, ResultSetMetaData meta, ResultSet rs) throws SQLException {
        int k = 1;
        for (int i = 5; i <= meta.getColumnCount(); i++) {
            // 由于Oracle数据库的bolb字段对应pg的oid,而现在pt数据库提倡使用bytea，所以这里进行手动修改数据类型。
            if (Types.BLOB == meta.getColumnType(i)) {
                stmt.setBytes(k, rs.getBytes(i));
            } else {
                stmt.setObject(k, rs.getObject(i), meta.getColumnType(i));
            }
            k++;
        }
        // 设置rowID的值为null，那么应该在下一条中可以获取。
        if (null == rs.getString(3)) {
            rs.next();
            stmt.setObject(k, rs.getString(3), Types.VARCHAR);
        } else {
            stmt.setObject(k, rs.getString(3), Types.VARCHAR);
        }
        return stmt;
    }

    private PreparedStatement setSqlServerValue(PreparedStatement stmt, ResultSetMetaData meta, ResultSet rs) throws SQLException {
        int k = 1;
        for (int i = 4; i <= meta.getColumnCount(); i++) {
            stmt.setObject(k, rs.getObject(i));
            k++;
        }
        return stmt;
    }

    /**
     * 把16进制转为2进制字符串
     * @param hex
     * @return
     */
    private String hexToBinary(String hex) {
        int i = Integer.parseInt(hex, 16);
        return Integer.toBinaryString(i);
    }

    // 把从数据库中获取的16进制转为可用的16进制
    private String getSubHex(String hexChanges, List<String> coreColumns, Map<String, Object> columnIndexMap) {
        // 截取真正有用的部分（后面所有的0以及前面的FE都是没用的）
        for (int i = hexChanges.length() -1; i >=0 ; i--) {
            if ("0".equals(hexChanges.substring(i, i + 1))) {
                hexChanges = hexChanges.substring(0, i);
            } else {
                // 从右边开始，遇到第一不是0的字符时跳出循环
                // 并且去掉开头无用的FE
                hexChanges = hexChanges.substring(2, hexChanges.length());
                break;
            }
        }
        // 把16进制字符串转为2进制字符串
        String binaryStr = hexToBinary(hexChanges);

        // 判断源表字段
        int invalid = 1;
        boolean flag = false;
        for (Map.Entry<String, Object> entity : columnIndexMap.entrySet()) {
            if (entity.getValue().equals(CommonCons.CDC_TRAGET_COLMAP)) {
                // 获取到TARGET_COLMAP$字段后下次循环开始计数
                flag = true;
                continue;
            }
            // 如果本次循环中的字段名为源表字段，终止循环。
            if (entity.getValue().equals(coreColumns.get(0))) {
                break;
            }
            if (flag) {
                invalid++;
            }
        }
        // 去掉无用字段
        binaryStr = binaryStr.substring(0, binaryStr.length() - invalid);
        // 调转
        String binaryTem = new StringBuilder(binaryStr).reverse().toString();
        log.info("修改的字段的二进制描述："+ binaryTem);
        return binaryTem;
    }

    /**
     * 把数据库中的16进制转为可用的二进制
     * @param hexChanges
     * @param coreColumns
     * @return
     */
    private String getSubHex4SqlServer(String hexChanges, List<String> coreColumns) {
        // 把16进制字符串转为2进制字符串
        String binaryStr = hexToBinary(hexChanges);
        // 把二进制倒置
        String binaryTem = new StringBuilder(binaryStr).reverse().toString();
        int sub = coreColumns.size() - binaryTem.length();
        StringBuffer fillZero = new StringBuffer();
        for (int i = 0; i < sub; i++) {
            fillZero.append("0");
        }
        return new StringBuilder(binaryTem).append(fillZero).toString();

    }

    private List<Map<String, Object>> setMessage(String binaryStr, String tableName, String operation, ResultSet rs, List<Map<String,Object>> changeList, List<String> coreColumns) throws SQLException {
        // 添加修改的信息
        Map<String, Object> message = new HashMap<>();
        message.put("operation", operation);
        message.put("rowId", rs.getString(3));
        message.put("timestamp", rs.getString(2));
        String targetTable ="";
        StringBuilder targetColumns = new StringBuilder();
        // 确定对应修改过得字段有哪些
        Map<String, Object> changeData = new HashMap<>();
        if ("delete".equals(operation)) {
            // 如果是删除操作的话，没有可用的binaryStr
            for (int i = 0; i < coreColumns.size(); i++) {
                String columnName = coreColumns.get(i);
                String tableColumn = String.valueOf(columnMap.get(tableName + "." + columnName));
                // 放入对应的我们数据库中的表名和字段名
                changeData.put(tableColumn.split("\\.")[1], rs.getObject(i + 3 +1));
                targetTable = tableColumn.split("\\.")[0];
                targetColumns.append(tableColumn.split("\\.")[1]);
                targetColumns.append(",");
            }
        } else {
            for (int i = 0; i < binaryStr.length(); i++) {
                String index = binaryStr.substring(i, i + 1);
                if ("1".equals(index)) {
                    // 发生了改变，获取对应的字段名字
                    String columnName = coreColumns.get(i);
                    String tableColumn = String.valueOf(columnMap.get((tableName + "." + columnName).toUpperCase()));
                    // 放入对应的我们数据库中的表名和字段名
                    changeData.put(tableColumn.split("\\.")[1], rs.getObject(i + 4 +1));
                    targetTable = tableColumn.split("\\.")[0];
                    targetColumns.append(tableColumn.split("\\.")[1]);
                    targetColumns.append(",");
                }
            }
        }

        message.put("tableName", targetTable);
        // 修改过的
        message.put("columns", targetColumns.substring(0, targetColumns.length() - 1));
        if (!"delete".equals(operation)) {
            message.put("changeData", changeData);
        }
        changeList.add(message);
        return changeList;
    }

    private List<Map<String, Object>> setSqlServerMessage(String binaryStr, String tableName,String identifyColumn, String operation, ResultSet rs, List<Map<String,Object>> changeList, List<String> coreColumns) throws SQLException {
        // 添加修改的信息
        Map<String, Object> message = new HashMap<>();
        message.put("operation", operation);
        message.put(identifyColumn, rs.getString(4));
        message.put("timestamp", rs.getString(3));
        String targetTable ="";
        StringBuilder targetColumns = new StringBuilder();
        // 确定对应修改过得字段有哪些
        Map<String, Object> changeData = new HashMap<>();
        if ("delete".equals(operation)) {
            // 如果是删除操作的话，没有可用的binaryStr
            for (int i = 0; i < coreColumns.size(); i++) {
                String columnName = coreColumns.get(i);
                String tableColumn = String.valueOf(columnMap.get(tableName + "." + columnName));
                // 放入对应的我们数据库中的表名和字段名
                changeData.put(tableColumn.split("\\.")[1], rs.getObject(i + 3 +1));
                targetTable = tableColumn.split("\\.")[0];
                targetColumns.append(tableColumn.split("\\.")[1]);
                targetColumns.append(",");
            }
        } else {
            for (int i = 0; i < binaryStr.length(); i++) {
                String index = binaryStr.substring(i, i + 1);
                if ("1".equals(index)) {
                    // 发生了改变，获取对应的字段名字
                    String columnName = coreColumns.get(i);
                    String tableColumn = String.valueOf(columnMap.get(tableName + "." + columnName));
                    // 放入对应的我们数据库中的表名和字段名
                    changeData.put(tableColumn.split("\\.")[1], rs.getObject(i + 3 +1));
                    targetTable = tableColumn.split("\\.")[0];
                    targetColumns.append(tableColumn.split("\\.")[1]);
                    targetColumns.append(",");
                }
            }
        }

        message.put("tableName", targetTable);
        // 修改过的
        message.put("columns", targetColumns.substring(0, targetColumns.length() - 1));
        if (!"delete".equals(operation)) {
            message.put("changeData", changeData);
        }
        changeList.add(message);
        return changeList;
    }

    private void setMysqlMessage(String operation, String identifyColumn, String timestamp, String tableName,
                                                      String columns, JSONObject changeData, List<Map<String, Object>> changeList) {
        // 添加修改的信息
        Map<String, Object> message = new HashMap<>();
        message.put("operation", operation);
        message.put(identifyColumn, identifyColumn);
        message.put("timestamp", timestamp);
        message.put("tableName", tableName);
        message.put("columns", columns);
        if (!"delete".equals(operation)) {
            message.put("changeData", changeData);
        }
        changeList.add(message);
    }

    /**
     * 通过rabbitmq发送消息
     * @param changeList
     */
    private void sentMessage(List<Map<String, Object>> changeList) {
        //首先设置rabbitTemplate的值
        if (null == rabbitTemplate) {
            RabbitmqProperties getSenderProperties = new RabbitmqProperties();
            getSenderProperties.setHost(rabbitmqProperties.getSenderHost());
            getSenderProperties.setPort(rabbitmqProperties.getSenderPort());
            getSenderProperties.setUsername(rabbitmqProperties.getSenderUsername());
            getSenderProperties.setPassword(rabbitmqProperties.getSenderPassword());
            rabbitTemplate = rabbitmqService.getSender(getSenderProperties);
        }


        // 首先查询注册了监听的用户
        String sql = "select * from cdc_register order  by routing_key";
        List<Map<String, Object>> registerList = pgJdbcTemplate.queryForList(sql);
        // 遍历所有查到的数据，重新整合
        Set<String> validateSet = new HashSet<>();
        List<List<Map<String, Object>>> resultList = new ArrayList<>();
        List<Map<String, Object>> entityList = new ArrayList<>();
        for (int i = 0; i < registerList.size(); i++) {
            Map<String, Object> register = registerList.get(i);
            String routingKey = String.valueOf(register.get("routing_key"));
            String tableName = String.valueOf(register.get("table_name"));
            boolean result = validateSet.add(routingKey + tableName);
            if (result && entityList.size() > 0) {
                // 如果添加成功的话，说明是新的集合
                resultList.add(entityList);
                entityList = new ArrayList<>();
                entityList.add(register);
            } else {
                entityList.add(register);
            }
            // 最后一条，直接添加
            if (i == registerList.size() -1) {
                resultList.add(entityList);
            }
        }

        Map<String, Object> exchangeMap = new LinkedHashMap<>();
        // 遍历所有的修改数据
        for (int i = 0; i < changeList.size(); i++) {
            // key->发送rabbitmq对应的routingkey  value-> 消息
            Map<String, Object> changeEntity = changeList.get(i);
            Map<String, Object> oldChangeData = (Map<String, Object>) changeEntity.get("changeData");
            String[] oldColumns = String.valueOf(changeEntity.get("columns")).split(",");
            Set<String> oldColumnsSet = new HashSet<>();
            Collections.addAll(oldColumnsSet, oldColumns);

            for (List<Map<String, Object>> maps : resultList) {
                Map<String, Object> changeEntityNew = new HashMap<>();
                changeEntityNew.putAll(changeEntity);
                StringBuilder columns = new StringBuilder();
                Map<String, Object> changeData = new HashMap<>();
                boolean flag = false;
                String routingKey = "";
                for (Map<String, Object> registerEntity : maps) {
                    String table_name = String.valueOf(registerEntity.get("table_name"));
                    String[] operationArray = String.valueOf(registerEntity.get("operation")).split(",");
                    Set<String> operation = new HashSet<>();
                    Collections.addAll(operation,operationArray);
                    if (table_name.equals(changeEntity.get("tableName")) && (operation.contains(String.valueOf(changeEntity.get("operation"))) || operation.contains("all")) && oldColumnsSet.contains(String.valueOf(registerEntity.get("column_name")))) {
                        flag = true;
                        routingKey = String.valueOf(registerEntity.get("routing_key"));
                        columns.append(registerEntity.get("column_name"));
                        columns.append(",");
                        if (!"delete".equals(changeEntity.get("operation"))) {
                            changeData.put(String.valueOf(registerEntity.get("column_name")), oldChangeData.get(registerEntity.get("column_name")));
                            changeEntityNew.put("changeData", changeData);
                        }
                        changeEntityNew.put("columns", columns);
                    }
                }
                if (flag) {
                    // 如果下条记录不是当前exchange的话，清除相关信息
                    if (exchangeMap.containsKey(routingKey)) {
                        List<Map<String, Object>> messages;
                        messages = (List<Map<String, Object>>) exchangeMap.get(routingKey);
                        messages.add(changeEntityNew);
                        exchangeMap.put(routingKey, messages);
                    } else {
                        // 如果存在exchange_key的话，取出来继续赋值
                        List<Map<String, Object>> messages = new ArrayList<>();
                        messages.add(changeEntityNew);
                        exchangeMap.put(routingKey, messages);
                    }
                }
            }
        }

        // 遍历通过rabbitmq发送消息
        for (Map.Entry<String, Object> stringObjectEntry : exchangeMap.entrySet()) {
            String routingKey = stringObjectEntry.getKey();
            String message = JSON.toJSONString(stringObjectEntry.getValue());
            rabbitTemplate.convertAndSend(rabbitmqProperties.getSenderExchange(),routingKey,message);
            System.out.println(routingKey);
            System.out.println(message);
        }


    }
}
