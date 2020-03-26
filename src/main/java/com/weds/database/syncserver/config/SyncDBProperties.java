package com.weds.database.syncserver.config;


import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 项目启动时检测是否需要初始化数据库的配置类
 */
@ConfigurationProperties(prefix = "spring.db.core")
public class SyncDBProperties {

    private boolean coreInit;
    // 要连接的核心数据库类型 1->oracle 2->sql server 3-> mysql
    private int coreDbType;

    // 执行的线程数
    private int coreThreadNum;

    private String username;

    private String password;

    private String driverClassName;

    private String url;

    public SyncDBProperties() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public boolean isCoreInit() {
        return coreInit;
    }

    public void setCoreInit(boolean coreInit) {
        this.coreInit = coreInit;
    }

    public int getCoreThreadNum() {
        return coreThreadNum;
    }

    public void setCoreThreadNum(int coreThreadNum) {
        this.coreThreadNum = coreThreadNum;
    }

    public int getCoreDbType() {
        return coreDbType;
    }

    public void setCoreDbType(int coreDbType) {
        this.coreDbType = coreDbType;
    }

}
