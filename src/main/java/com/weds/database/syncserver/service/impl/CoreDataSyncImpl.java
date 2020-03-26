package com.weds.database.syncserver.service.impl;

import com.weds.database.syncserver.config.SyncDBProperties;
import com.weds.database.syncserver.cons.CommonCons;
import com.weds.database.syncserver.service.CoreDataSync;
import com.weds.database.syncserver.service.ScheduleService;
import com.weds.database.syncserver.utils.ConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CoreDataSyncImpl implements CoreDataSync {

    @Resource
    private ConnectionFactory connectionFactory;
    @Resource
    private SyncDBProperties syncDBProperties;

    @Resource(name = "pgJdbcTemplate")
    private JdbcTemplate pgJdbcTemplate;

    @Resource(name = "sqlServerJdbcTemplate")
    private JdbcTemplate sqlServerJdbcTemplate;

    @Resource(name = "oracleJdbcTemplate")
    private JdbcTemplate oracleJdbcTemplate;

    @Resource
    private ScheduleService scheduleService;

    private AtomicLong currentSynCount; // 当前已同步的条数
    private Logger log = Logger.getLogger(getClass());
    private long totalNum;
    private Connection coreConnection;
    private Connection targetConn;

    @SuppressWarnings("unchecked")
    @Override
    public void syncData() throws Exception {

        // 从配置表获取信息
        Map<String, Object> configMap = getCdcConfig();
        List<String> coreTableNameList = (List<String>) configMap.get("coreTableNameList");
        List<String> targetTableNameList = (List<String>) configMap.get("targetTableNameList");
        List<String> coreUser = (List<String>) configMap.get("coreUser");
        Map<String, List<String>> coreTables = (Map<String, List<String>>) configMap.get("coreTables");
        Map<String, List<String>> targetTables = (Map<String, List<String>>) configMap.get("targetTables");

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // 初始化数据同步时间条件
        String confirmSql;
        Map<String, Object> conditionMap;
        String initTime;
        switch (syncDBProperties.getCoreDbType()) {
            case 1:// oracle
                if (syncDBProperties.isCoreInit()) {// 是初始化的，查询数据库当前的时间
                    String coreSql = "select to_char(systimestamp, 'yyyy-mm-dd hh24:mi:ss.ff3') as TIME_NOW from dual";
                    conditionMap = oracleJdbcTemplate.queryForMap(coreSql);
                    initTime = conditionMap.get("TIME_NOW").toString();
                    Date initDate = sdf.parse(initTime);
                    Timestamp timestamp = new Timestamp(initDate.getTime());
                    // 删除pg数据库中原来flag是Oracle的所有记录
                    pgJdbcTemplate.update("delete from cdc_update_timestamp where flag = 'oracle'");
                    // 获取到数据库时间后，把时间跟所有的源表相结合，存入pg数据库中
                    for (int i = 0; i < coreTableNameList.size(); i++) {
                        String coreTableName = coreTableNameList.get(i);
                        pgJdbcTemplate.update("insert into cdc_update_timestamp (condition_time, flag, core_table_name) VALUES (?,?,?)",(PreparedStatement ps) -> {
                            ps.setTimestamp(1, timestamp);
                            ps.setObject(2, "oracle");
                            ps.setObject(3, coreTableName);
                        });
                    }
                }
                break;
            case 2: // sql server
                if (syncDBProperties.isCoreInit()) {// 是初始化的，查询数据库当前的时间
                    String coreSql = "select convert(char(24),getdate(),121) as TIME_NOW";
                    conditionMap = sqlServerJdbcTemplate.queryForMap(coreSql);
                    initTime = conditionMap.get("TIME_NOW").toString();
                    Date initDate = sdf.parse(initTime);
                    Timestamp timestamp = new Timestamp(initDate.getTime());
                    // 删除pg数据库中原来flag是Oracle的所有记录
                    pgJdbcTemplate.update("delete from cdc_update_timestamp where flag = 'sql_server'");
                    // 获取到数据库时间后，把时间跟所有的源表相结合，存入pg数据库中
                    for (int i = 0; i < coreTableNameList.size(); i++) {
                        String coreTableName = coreTableNameList.get(i);
                        pgJdbcTemplate.update("insert into cdc_update_timestamp (condition_time, flag, core_table_name) VALUES (?,?,?)",(PreparedStatement ps) -> {
                            ps.setTimestamp(1, timestamp);
                            ps.setObject(2, "sql_server");
                            ps.setObject(3, coreTableName);
                        });
                    }
                }
                break;

            default:
                break;
        }

        // 判断配置文件中是否需要初始化数据库
        if (!syncDBProperties.isCoreInit()) {
            log.info("不需要初始化数据库");
            // 开始监听
            scheduleService.listenDbChanges();
            return;
        }

        // 获取线程数
        int coreThreadNum = syncDBProperties.getCoreThreadNum();

        for (int i = 0; i < coreTableNameList.size(); i++) {

            // 获取要同步的字段
            List<String> coreColumnsUsed = coreTables.get(coreTableNameList.get(i));
            List<String> targetColumnsUsed = targetTables.get(targetTableNameList.get(i));
            StringBuilder selectColumn = new StringBuilder();
            for (int j = 0; j < coreColumnsUsed.size(); j++) {
                selectColumn.append("a." + coreColumnsUsed.get(j));
                if (j != coreColumnsUsed.size() - 1) {
                    selectColumn.append(",");
                }
            }

            log.info("开始同步核心库" + coreTableNameList.get(i) + "表数据");

            currentSynCount = new AtomicLong(0L);
            // 获得核心库连接
            coreConnection = connectionFactory.getInitConnection(3);
            Statement coreStmtCount = coreConnection.createStatement();
            coreConnection.setAutoCommit(false);// 设置手动提交
            // 获得目标库连接
            targetConn = connectionFactory.getInitConnection(1);
            targetConn.setAutoCommit(false);// 设置手动提交
            // 锁表
            String countSql;
            switch (syncDBProperties.getCoreDbType()) {
                case 1: // oracle
                    // 执行锁表操作,执行的是共享模式，这样可以开启多个线程，独占模式导致进程被阻塞。
                    boolean lockRen = coreStmtCount.execute("LOCK TABLE "+ coreTableNameList.get(i) +" IN SHARE MODE");
                    log.info("锁表操作返回：" + lockRen);
                    countSql = "SELECT count(*) FROM "+coreTableNameList.get(i);
                    break;
                case 2: //sql server
                    log.info("sql server 锁表不需要执行额外方法");
                    countSql = "SELECT count(*) FROM "+coreTableNameList.get(i) + " WITH (TABLOCK, HOLDLOCK)";
                    break;
                case 3: // mysql
                    // 锁表
                    coreStmtCount.execute("LOCK TABLE "+ coreUser.get(i) + "." + coreTableNameList.get(i) +" WRITE, " + coreUser.get(i) + "." + coreTableNameList.get(i) + " AS a READ");
                    countSql = "SELECT count(*) FROM "+ coreUser.get(i) + "." + coreTableNameList.get(i);
                    break;
                default:
                    throw new Exception("未定义实现方法");
            }
            // 为每个线程分配结果集
            ResultSet coreRs = coreStmtCount.executeQuery(countSql);
            coreRs.next();
            // 总共处理的数量
            totalNum = coreRs.getLong(1);
            // 每个线程处理的数量
            long ownerRecordNum = ((totalNum % coreThreadNum) == 0) ? (totalNum / coreThreadNum) : (((totalNum +1)  / coreThreadNum));
            log.info("共需要同步的数据量："+totalNum);
            log.info("同步线程数量："+coreThreadNum);
            log.info("每个线程可处理的数量："+ownerRecordNum);
            // 执行锁表操作
            log.info("执行锁表操作：");
            log.info("数据库类型：" + syncDBProperties.getCoreDbType());
            Instant beginTime = Instant.now();

            CountDownLatch countdown = new CountDownLatch(coreThreadNum);
            // 开启五个线程向目标库同步数据
            for(int k=0; k < coreThreadNum; k ++){
                // 如果是最后一次循环的话，增加的数据量根据总数据量计算
                String sqlBuilder;
                switch (syncDBProperties.getCoreDbType()) {
                    case 1: //oracle
                        if (k == coreThreadNum - 1) {
                            sqlBuilder = "Select * From ( select " + selectColumn + ",a.ROWID as row_id, ROWNUM rnum from " + coreUser.get(i) +"." + coreTableNameList.get(i) +
                                    " a where rownum <= " + totalNum +
                                    " ) where rnum > " +
                                    (k * ownerRecordNum);
                        } else {
                            sqlBuilder = "Select * From ( select " + selectColumn + ",a.ROWID as row_id, ROWNUM rnum from " + coreUser.get(i) +"." + coreTableNameList.get(i) +
                                    " a where rownum <= " + (k * ownerRecordNum + ownerRecordNum) +
                                    " ) where rnum > " +
                                    (k * ownerRecordNum);
                        }
                        System.out.println("sql" + sqlBuilder);
                        break;
                    case 2: // sql server
                        if (k == coreThreadNum - 1) {
                            sqlBuilder = "Select * From ( select "
                                    + selectColumn + " ,ROW_NUMBER() OVER ( ORDER BY id ) AS RowNum from " + coreUser.get(i) +"." + coreTableNameList.get(i) + " a) AS b WHERE RowNum >"
                                    + (k * ownerRecordNum) + " AND RowNum <=" +totalNum;

                        } else {
                            sqlBuilder = "Select * From ( select "
                                    + selectColumn + " ,ROW_NUMBER() OVER ( ORDER BY id ) AS RowNum from " + coreUser.get(i) +"." + coreTableNameList.get(i) + " a) AS b WHERE RowNum >"
                                    + (k * ownerRecordNum) + " AND RowNum <=" +(k * ownerRecordNum + ownerRecordNum);
                        }
                        System.out.println("sql" + sqlBuilder);
                        break;
                    case 3: // mysql
                        if (k == coreThreadNum - 1) {
                            sqlBuilder = "Select " + selectColumn
                                    + " from " + coreUser.get(i) +"." + coreTableNameList.get(i) + " a LIMIT "
                                    + (k * ownerRecordNum) + " ," + (totalNum - (k * ownerRecordNum));
                        } else {
                            sqlBuilder = "Select " + selectColumn
                                    + " from " + coreUser.get(i) +"." + coreTableNameList.get(i) + " a LIMIT "
                                    + (k * ownerRecordNum) + " ," +ownerRecordNum;
                        }
                        break;
                    default:
                        throw new Exception("未定义数据同步类型");

                }
                // 拼装后SQL示例
                // select * from DT_USER_TEST OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY
                Thread workThread = new Thread(
                        new WorkerHandler(sqlBuilder, coreTableNameList.get(i),targetTableNameList.get(i),targetColumnsUsed, this, countdown));
                workThread.setName("SyncThread-"+k);
                workThread.start();
            }
            countdown.await();//等待所有工人完成工作
            // 恢复后释放连接
            if (3 == syncDBProperties.getCoreDbType()) {
                coreStmtCount.execute("UNLOCK TABLES");
            }
            connectionFactory.release(coreConnection, coreStmtCount);
            connectionFactory.release(targetConn, null);

            // 计算时间差
            Instant endTime = Instant.now();
            Duration duration = Duration.between(beginTime, endTime);
            log.info("同步所耗时：" + duration.toMillis());
        }
        log.info("所有表数据同步完成！");
        // 开始监听
        scheduleService.listenDbChanges();
    }

    @Override
    public Map<String, Object> getCdcConfig() throws Exception {
        Map<String, Object> returnMap = new HashMap<>();
        // 从数据库中查询需要同步的表名和字段
        String configSql = "select * from cdc_config where sys_flag = ? ORDER BY core_table,seq";
        List<Map<String, String>> configList = pgJdbcTemplate.query(configSql, new String[] {CommonCons.SERVER_NAME},(rs, rowNum) -> {
            Map<String, String> config = new HashMap<>();
            config.put("id", rs.getString("id"));
            config.put("coreUser", rs.getString("core_user"));
            config.put("coreTable", rs.getString("core_table"));
            config.put("coreColumn", rs.getString("core_column"));
            config.put("targetTable", rs.getString("target_table"));
            config.put("targetColumn", rs.getString("target_column"));
            config.put("sysFlag", rs.getString("sys_flag"));
            config.put("coreColumnType", rs.getString("core_column_type"));
            return config;
        });
        // 便利集合，得到表的集合以及列的集合
        LinkedHashSet coreTableName = new LinkedHashSet();
        List<String> coreUser = new ArrayList<>();
        LinkedHashSet<String> targetTableName = new LinkedHashSet();
        // 核心表
        Map<String, List<String>> coreTables = new HashMap<>();
        List<String> coreColumns = new ArrayList<>();
        List<String> coreColumnTypes = new ArrayList<>();
        // 目标表
        Map<String, List<String>> targetTables = new HashMap<>();
        List<String> targetColumns = new ArrayList<>();
        // 核心表字段跟目标表字段的对应关系
        Map<String, Object> columnMap = new HashMap<>();
        for (Map<String, String> config : configList) {
            // 放入表名
            boolean addFlag = coreTableName.add(config.get("coreTable"));
            targetTableName.add(config.get("targetTable"));
            if (addFlag) {
                coreUser.add(config.get("coreUser"));
            }
            // 核心字段和目标字段的对应关系
            columnMap.put(config.get("coreTable") + "." + config.get("coreColumn"), config.get("targetTable") + "." + config.get("targetColumn"));
            // 存入核心表和对应的column
            if (coreTables.containsKey(config.get("coreTable"))) {
                coreColumns.add(config.get("coreColumn"));
                coreTables.put(config.get("coreTable"), coreColumns);
            } else {
                coreColumns = new ArrayList<>();
                coreColumns.add(config.get("coreColumn"));
                coreTables.put(config.get("coreTable"), coreColumns);
            }
            // 存入核心表对应的字段类型
            if (coreTables.containsKey(config.get("coreTable") + CommonCons.CORE_COLUMN_TYPE)) {
                coreColumnTypes.add(config.get("coreColumnType"));
                coreTables.put(config.get("coreTable") + CommonCons.CORE_COLUMN_TYPE, coreColumnTypes);
            } else {
                coreColumnTypes = new ArrayList<>();
                coreColumnTypes.add(config.get("coreColumnType"));
                coreTables.put(config.get("coreTable") + CommonCons.CORE_COLUMN_TYPE, coreColumnTypes);
            }


            // 存入目标表和对应的column
            if (targetTables.containsKey(config.get("targetTable"))) {
                targetColumns.add(config.get("targetColumn"));
                targetTables.put(config.get("targetTable"), targetColumns);
            } else {
                targetColumns = new ArrayList<>();
                targetColumns.add(config.get("targetColumn"));
                targetTables.put(config.get("targetTable"), targetColumns);
            }

        }
        if (targetTables.size() != coreTables.size() -targetTables.size() || coreTableName.size() != targetTableName.size()) {
            log.error("cdc_config 表配置有误！");
            throw new Exception("cdc_config 表配置有误！");
        }
        // 把表名的set集合转为list
        List<String> coreTableNameList = new ArrayList<>();
        coreTableNameList.addAll(coreTableName);
        List<String> targetTableNameList = new ArrayList<>();
        targetTableNameList.addAll(targetTableName);
        List<String> coreUserList = new ArrayList<>();
        coreUserList.addAll(coreUser);
        returnMap.put("coreTableNameList", coreTableNameList);
        returnMap.put("targetTableNameList", targetTableNameList);
        returnMap.put("coreTables", coreTables);
        returnMap.put("targetTables", targetTables);
        returnMap.put("coreUser", coreUserList);
        returnMap.put("columnMap", columnMap);
        return returnMap;
    }

    //数据同步线程
    final class WorkerHandler implements Runnable {
        String queryStr;
        String coreTBName;
        String targetTBName;
        List<String> targetColumnsUsed;
        private Object toNotify;
        CountDownLatch countdown;
        public WorkerHandler(String queryStr,String coreTBName,String targetTBName, List<String> targetColumnsUsed, Object toNotify, CountDownLatch countdown) {
            this.queryStr = queryStr;
            this.targetTBName = targetTBName;
            this.toNotify = toNotify;
            this.coreTBName = coreTBName;
            this.targetColumnsUsed = targetColumnsUsed;
            this.countdown = countdown;
        }
        @Override
        public void run() {
            try {
                //开始同步
                launchSyncData();
            } catch(Exception e){
                log.error(e);
                e.printStackTrace();
            }
        }
        //同步数据方法
        void launchSyncData() throws Exception{

            Statement coreStmt = coreConnection.createStatement();
            ResultSet coreRs = coreStmt.executeQuery(queryStr);
            log.info(Thread.currentThread().getName()+"'s Query SQL::"+queryStr);
            int batchCounter = 0; //累加的批处理数量

            // 获取元数据
            ResultSetMetaData meta = coreRs.getMetaData();
            // 声明行名字和值
            StringBuilder columnNames = new StringBuilder();
            StringBuilder bindVariables = new StringBuilder();
            // 拼装sql
            for (int i = 0; i < targetColumnsUsed.size(); i++) {
                columnNames.append(targetColumnsUsed.get(i));
                columnNames.append(", ");
                bindVariables.append('?');
                bindVariables.append(", ");
            }
            switch (syncDBProperties.getCoreDbType()) {
                case 1: // oracle
                    // 追加row_id
                    columnNames.append(CommonCons.ROW_ID);
                    bindVariables.append('?');
                    break;
                case 2: // sql server
                    columnNames = new StringBuilder(columnNames.substring(0, columnNames.length() - 2));
                    bindVariables = new StringBuilder(bindVariables.substring(0, bindVariables.length() - 2));
                    break;
                case 3: // mysql
                    columnNames = new StringBuilder(columnNames.substring(0, columnNames.length() - 2));
                    bindVariables = new StringBuilder(bindVariables.substring(0, bindVariables.length() - 2));
                    break;
                default:
                    break;
            }

            String sql = "INSERT INTO " + targetTBName + " ("
                    + columnNames
                    + ") VALUES ("
                    + bindVariables
                    + ")";
            PreparedStatement targetPstmt = targetConn.prepareStatement(sql);
            while (coreRs.next()){
                switch (syncDBProperties.getCoreDbType()) {
                    case 1: //oracle
                        // 设置占位的?对应的值以及数据类型
                        for (int i = 1; i < meta.getColumnCount(); i++) {
                            // 由于Oracle数据库的bolb字段对应pg的oid,而现在pt数据库提倡使用bytea，所以这里进行手动修改数据类型。
                            if (Types.BLOB == meta.getColumnType(i)) {
                                targetPstmt.setBytes(i, coreRs.getBytes(i));
                            } else if (CommonCons.ROW_ID.equals(meta.getColumnName(i))) { //rowId的话，特殊制定类型
                                targetPstmt.setObject(i, coreRs.getString(i), Types.VARCHAR);
                            } else {
                                targetPstmt.setObject(i, coreRs.getObject(i));
                            }
                        }
                        break;
                    case 2: // sql server
                        // 设置占位的?对应的值以及数据类型
                        for (int i = 1; i < meta.getColumnCount(); i++) {
                            if (CommonCons.ROW_NUM.equals(meta.getColumnName(i))) {
                                log.info("do nothing");
                            } else {
                                targetPstmt.setObject(i, coreRs.getObject(i));
                            }
                        }
                        break;
                    case 3: // mysql
                        // 设置占位的?对应的值以及数据类型
                        for (int i = 1; i <= meta.getColumnCount(); i++) {
                            targetPstmt.setObject(i, coreRs.getObject(i));
                        }
                        break;

                }

                targetPstmt.addBatch();
                batchCounter++;
                currentSynCount.incrementAndGet();//递增
                if (batchCounter % 10000 == 0) { //1万条数据一提交
                    targetPstmt.executeBatch();
                    targetPstmt.clearBatch();
                    targetConn.commit();
                }
            }
            //提交剩余的批处理
            targetPstmt.executeBatch();
            targetPstmt.clearBatch();
            targetConn.commit();
            log.info(queryStr);
            //释放连接
            connectionFactory.release(null, targetPstmt);
            countdown.countDown();
        }
    }


    private void doOracleSync() {

    }

    private void doSqlServerSync() {

    }
}
