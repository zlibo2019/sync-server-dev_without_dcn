package com.weds.database.syncserver.service;

import java.util.Map;

public interface CoreDataSync {

    void syncData() throws Exception;

    Map<String, Object> getCdcConfig() throws Exception;
}
