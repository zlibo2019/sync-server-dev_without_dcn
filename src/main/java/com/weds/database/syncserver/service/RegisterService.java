package com.weds.database.syncserver.service;

import com.weds.database.syncserver.config.RegisterProperties;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 注册接口
 */
public interface RegisterService {

    /**
     * 注册
     * @param param
     * @return
     */
    Map<String, Object> register(List<RegisterProperties> param) throws SQLException, IOException, TimeoutException;

    Map<String, Object> unRegister(List<RegisterProperties> param) throws SQLException;
}
