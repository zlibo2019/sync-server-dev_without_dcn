package com.weds.database.syncserver.controller;


import com.weds.database.syncserver.config.RegisterProperties;
import com.weds.database.syncserver.service.RegisterService;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 对外暴露的注册接口
 */
@RestController
@RequestMapping("/api")
public class RegisterAction {


    @Resource
    private RegisterService registerService;


    /**
     * 提供的注册接口
     * @param param
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws TimeoutException
     */
    @RequestMapping(value = "/register", method = RequestMethod.POST)
    public Map<String, Object> register(@RequestBody List<RegisterProperties> param) throws SQLException, IOException, TimeoutException {
        // 返回的数据对象，spring boot 会自动转换为Json字符串返回
        return registerService.register(param);
    }


    /**
     * 注销接口
     * @param param
     * @return
     */
    @RequestMapping(value = "/unRegister", method = RequestMethod.POST)
    public Map<String, Object> unRegister(@RequestBody List<RegisterProperties> param) throws SQLException {
        return registerService.unRegister(param);

    }

}
