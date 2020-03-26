package com.weds.database.syncserver.config;

import com.weds.framework.mq.autoconfiguration.RabbitmqParam;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.rabbitmq")
public class RabbitmqProperties extends RabbitmqParam {

    // 数据库名称
    private String database;

    // 发布订阅用的exchange
    private String subExchange;

    // 发布订阅用的queue名
    private String subQueue;

    // 批量操作时间间隔
    private int commitDuration;

    private String senderHost;

    private int senderPort;

    private String senderUsername;

    private String senderPassword;

    private boolean senderQueueDurable;

    private boolean senderExchangeDurable;

    private String senderExchange;


    public RabbitmqProperties() {
    }

    public boolean isSenderQueueDurable() {
        return senderQueueDurable;
    }

    public void setSenderQueueDurable(boolean senderQueueDurable) {
        this.senderQueueDurable = senderQueueDurable;
    }

    public boolean isSenderExchangeDurable() {
        return senderExchangeDurable;
    }

    public void setSenderExchangeDurable(boolean senderExchangeDurable) {
        this.senderExchangeDurable = senderExchangeDurable;
    }

    public String getSenderExchange() {
        return senderExchange;
    }

    public void setSenderExchange(String senderExchange) {
        this.senderExchange = senderExchange;
    }

    public String getSenderHost() {
        return senderHost;
    }

    public void setSenderHost(String senderHost) {
        this.senderHost = senderHost;
    }

    public int getSenderPort() {
        return senderPort;
    }

    public void setSenderPort(int senderPort) {
        this.senderPort = senderPort;
    }

    public String getSenderUsername() {
        return senderUsername;
    }

    public void setSenderUsername(String senderUsername) {
        this.senderUsername = senderUsername;
    }

    public String getSenderPassword() {
        return senderPassword;
    }

    public void setSenderPassword(String senderPassword) {
        this.senderPassword = senderPassword;
    }

    public String getSubExchange() {
        return subExchange;
    }

    public void setSubExchange(String subExchange) {
        this.subExchange = subExchange;
    }

    public String getSubQueue() {
        return subQueue;
    }

    public void setSubQueue(String subQueue) {
        this.subQueue = subQueue;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getCommitDuration() {
        return commitDuration;
    }

    public void setCommitDuration(int commitDuration) {
        this.commitDuration = commitDuration;
    }
}
