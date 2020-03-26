package com.weds.database.syncserver.startup;

import com.weds.database.syncserver.service.CoreDataSync;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 项目启动后执行该类
 * 初始化DCN服务，监听所有表的变更
 */
@Component
@Transactional
public class StartRunner{

    @Resource
    private CoreDataSync coreDataSync;

    @PostConstruct
    public void init() {
        try {
            // 同步数据操作，方法内会判断是否需要执行
            coreDataSync.syncData();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
