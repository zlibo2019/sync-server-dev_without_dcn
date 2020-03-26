package com.weds.database.syncserver.utils;

import com.weds.database.syncserver.config.SyncDBProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.*;

@Component
public class ConnectionFactory {

    // oracle数据库连接
    @Resource(name = "oracleJdbcTemplate")
    private JdbcTemplate oracleJdbcTemplate;

    @Resource
    private SyncDBProperties syncDBProperties;

    @Resource(name = "sqlServerJdbcTemplate")
    private JdbcTemplate sqlServerJdbcTemplate;

    // pg数据库连接
    @Resource(name = "pgJdbcTemplate")
    private JdbcTemplate pgJdbcTemplate;

    public Connection getInitConnection(int connType) throws Exception {
        switch (connType) {
            case 1:// 获取postgresql数据库连接
                return pgJdbcTemplate.getDataSource().getConnection();
            case 2:// 获取oracle数据库连接
                return oracleJdbcTemplate.getDataSource().getConnection();
            case 3:// 获取同步数据时的数据库连接
                Class.forName(syncDBProperties.getDriverClassName());//指定连接类型
                return DriverManager.getConnection(syncDBProperties.getUrl(), syncDBProperties.getUsername(), syncDBProperties.getPassword());
            case 4:// 获取sqlServer的数据库连接
                return sqlServerJdbcTemplate.getDataSource().getConnection();
            default:
                throw new Exception("connType参数错误！");
        }
    }

    /**
     * 释放连接
     * @param conn
     * @param statement
     * @param resultSet
     */
    public void release(Connection conn, Statement statement, ResultSet resultSet) throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != statement) {
            statement.close();
        }
        if (null != resultSet) {
            resultSet.close();
        }
    }


    /**
     * 释放连接
     * @param conn
     * @param statement
     * @throws SQLException
     */
    public void release(Connection conn, Statement statement) throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != statement) {
            statement.close();
        }
    }

    public void release(Connection conn, Statement statement1, Statement statement2) throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != statement1) {
            statement1.close();
        }
        if (null != statement2) {
            statement2.close();
        }
    }

    public void release(Connection conn, Statement statement1, Statement statement2, Statement statement3) throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != statement1) {
            statement1.close();
        }
        if (null != statement2) {
            statement2.close();
        }
        if (null != statement3) {
            statement3.close();
        }
    }
}
