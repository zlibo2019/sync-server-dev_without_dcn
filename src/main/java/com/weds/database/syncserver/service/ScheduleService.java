package com.weds.database.syncserver.service;

import java.util.Map;

/**
 * 计划任务服务接口
 */
public interface ScheduleService {


    /**
     * 计划任务
     * 监听数据库数据改变
     * @return
     */
    void listenDbChanges() throws Exception;
}
