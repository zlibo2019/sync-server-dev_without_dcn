package com.weds.database.syncserver.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.weds.database.syncserver.config.RabbitmqProperties;
import com.weds.database.syncserver.config.RegisterProperties;
import com.weds.database.syncserver.cons.CommonCons;
import com.weds.database.syncserver.service.RegisterService;
import com.weds.framework.mq.RabbitmqService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeoutException;

@Service
public class RegisterServiceImpl implements RegisterService {

    @Resource
    private RabbitmqService rabbitmqService;
    @Resource
    private RabbitmqProperties rabbitmqProperties;

    @Resource(name = "pgJdbcTemplate")
    private JdbcTemplate pgJdbcTemplate;

    /**
     * 注册
     *
     * @param param
     * @return
     */
    @Override
    public Map<String, Object> register(List<RegisterProperties> param) throws SQLException, IOException, TimeoutException {
        // 返回的map
        Map<String, Object> returnMap = new HashMap<>();
        JSONObject data = new JSONObject();
        String routingKey = null;
        String queueName = null;
        try {
            UUID uuid = UUID.randomUUID();
            // 生成routingkey
            routingKey = uuid.toString().replace("-", "");
            queueName = "queue_" + routingKey;
            // 遍历所有要注册的字段
            for (RegisterProperties entity : param) {
                String insertSql = "INSERT INTO " + CommonCons.REGISTER_TABLE_NAME + "( ROUTING_KEY,TABLE_NAME,COLUMN_NAME,OPERATION,IS_DETAIL) VALUES (?,?,?,?,?)";
                List<Object[]> listValue = new ArrayList<>();
                // 判断是否要监听表的所有字段
                if (entity.isFullTable() || Strings.isNullOrEmpty(entity.getColumnName())) {
                    // 查询表的所有字段
                    String sql = "SELECT * FROM " + entity.getTableName() + " LIMIT 0";
                    SqlRowSet sqlRowSet = pgJdbcTemplate.queryForRowSet(sql);
                    SqlRowSetMetaData sqlRowSetMetaData = sqlRowSet.getMetaData();
                    for (int i = 0; i < sqlRowSetMetaData.getColumnCount(); i++) {
                        String key = sqlRowSetMetaData.getColumnName(i + 1);
                        Object[] valueEntity = new Object[5];
                        // 把每一个column存入记录中
                        valueEntity[0] = routingKey;
                        valueEntity[1] = entity.getTableName();
                        valueEntity[2] = key;
                        valueEntity[3] = entity.getOperation();
                        valueEntity[4] = entity.isDetail();
                        listValue.add(valueEntity);
                    }
                    pgJdbcTemplate.batchUpdate(insertSql, listValue);
                } else {
                    Object[] valueEntity = new Object[5];
                    valueEntity[0] = routingKey;
                    valueEntity[1] = entity.getTableName();
                    valueEntity[2] = entity.getColumnName();
                    valueEntity[3] = entity.getOperation();
                    valueEntity[4] = entity.isDetail();
                    listValue.add(valueEntity);
                    pgJdbcTemplate.batchUpdate(insertSql,listValue);
                }

            }
            // 去rabbitmq 发布主题
            RabbitmqProperties subProperties = new RabbitmqProperties();
            subProperties.setQueueName(queueName);
            subProperties.setExchange(rabbitmqProperties.getSenderExchange());
            subProperties.setHost(rabbitmqProperties.getSenderHost());
            subProperties.setPort(rabbitmqProperties.getSenderPort());
            subProperties.setQueueDurable(rabbitmqProperties.isSenderQueueDurable());
            subProperties.setExchangeDurable(rabbitmqProperties.isSenderExchangeDurable());
            subProperties.setUsername(rabbitmqProperties.getSenderUsername());
            subProperties.setPassword(rabbitmqProperties.getSenderPassword());
            subProperties.setRoutingKey(routingKey);
            subProperties.setExchangeType("direct");
            rabbitmqService.publishTopic(subProperties);
        } catch (DataAccessException e) {
            e.printStackTrace();
            data.put("exchange", rabbitmqProperties.getSubExchange());
            data.put("queue", queueName);
            data.put("routingKey", routingKey);
            returnMap.put("result", "-1");
            returnMap.put("msg", e.getMessage());
            return returnMap;
        }
        data.put("exchange", rabbitmqProperties.getSubExchange());
        data.put("queue", queueName);
        data.put("routingKey", routingKey);
        returnMap.put("result", "1");
        returnMap.put("data", data.toJSONString());
        returnMap.put("msg", "注册成功！");
        return returnMap;
    }


    /**
     * 取消注册方法
     * @return
     */
    @Override
    public Map<String, Object> unRegister(List<RegisterProperties> param) throws SQLException {
        Map<String, Object> returnMap = new HashMap<>();
        pgJdbcTemplate.getDataSource().getConnection().setAutoCommit(false);
        // 遍历要取消注册的集合
        for (RegisterProperties entity : param) {
            // 如果是整张表
            if (entity.isFullTable() || Strings.isNullOrEmpty(entity.getColumnName())) {
                String delSql = "DELETE FROM CDC_REGISTER " + "WHERE ROUTING_KEY = ? AND TABLE_NAME = ?";
                pgJdbcTemplate.update(delSql, (PreparedStatement ps) -> {
                    ps.setString(1, entity.getRoutingKey());
                    ps.setString(2, entity.getTableName());
                });
            } else {
                String delSql = "DELETE FROM CDC_REGISTER " +"WHERE routing_key = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                pgJdbcTemplate.update(delSql, (PreparedStatement ps) -> {
                    ps.setString(1, entity.getRoutingKey());
                    ps.setString(2, entity.getTableName());
                    ps.setString(3, entity.getColumnName());
                });
            }
        }
        returnMap.put("result", "1");
        returnMap.put("msg", "注销成功！");
        return returnMap;
    }
}
