package com.weds.database.syncserver.controller;

import com.alibaba.fastjson.JSON;
import com.weds.database.syncserver.config.RegisterProperties;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class RegisterActionTest {



//    @Test
    public void changeData() throws IOException {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost("http://localhost:8080/test/changeData");
        request.addHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(request);
        System.out.println("response");
        System.out.println(response);
    }

//    @Test
    public void testInsert() throws Exception {
//        Class.forName("com.mysql.jdbc.Driver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:mysql://10.0.0.96:3123/testduliyan", "root", "123456");
//        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:sqlserver://10.2.0.60\\\\MSSQLSERVER:1433;database=CDC_DB", "sa", "123456");
        int id = 625566;
        Class.forName("oracle.jdbc.driver.OracleDriver");//指定连接类型
        Connection targetConn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.0.16:1521:orcl", "cdc_publisher", "cdc_publisher");
        targetConn.setAutoCommit(false);
        PreparedStatement statement = targetConn.prepareStatement("insert into SCOTT.DT_USER_TEST (ID,USER_SERIAL,USER_LNAME, USER_DEP) VALUES (?,?, ?, ?)");
        for (int i = 0; i < 100; i++) {
            statement.setObject(1, id + i);
            statement.setObject(2,i );
            statement.setObject(3, "wxl");
            statement.setObject(4, i);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.clearBatch();
        targetConn.commit();
    }

//    @Test
    public void testUpdate() throws Exception {
        long id = 625566;
//        Class.forName("com.mysql.jdbc.Driver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:mysql://10.0.0.96:3123/testduliyan", "root", "123456");
//        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:sqlserver://10.2.0.60\\\\MSSQLSERVER:1433;database=CDC_DB", "sa", "123456");
        Class.forName("oracle.jdbc.driver.OracleDriver");//指定连接类型
        Connection targetConn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.0.16:1521:orcl", "cdc_publisher", "cdc_publisher");
        targetConn.setAutoCommit(false);
        PreparedStatement statement = targetConn.prepareStatement("update SCOTT.DT_USER_TEST set USER_LNAME = 'update' where id = ?");
        for (int i = 0; i < 100; i++) {
            statement.setObject(1, id + i);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.clearBatch();
        targetConn.commit();
        targetConn.close();
    }

//    @Test
    public void testDel() throws Exception {
        long id = 625566;
//        Class.forName("com.mysql.jdbc.Driver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:mysql://10.0.0.96:3123/testduliyan", "root", "123456");
//        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");//指定连接类型
//        Connection targetConn = DriverManager.getConnection("jdbc:sqlserver://10.2.0.60\\\\MSSQLSERVER:1433;database=CDC_DB", "sa", "123456");
        Class.forName("oracle.jdbc.driver.OracleDriver");//指定连接类型
        Connection targetConn = DriverManager.getConnection("jdbc:oracle:thin:@10.2.0.16:1521:orcl", "cdc_publisher", "cdc_publisher");
        targetConn.setAutoCommit(false);
        PreparedStatement statement = targetConn.prepareStatement("delete from SCOTT.DT_USER_TEST where id = ?");
        for (int i = 0; i < 100; i++) {
            statement.setObject(1, id + i);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.clearBatch();
        targetConn.commit();
        targetConn.close();
    }

//    @Test
    public void register() throws Exception {
        List<RegisterProperties> list = new ArrayList<>();
        RegisterProperties param = new RegisterProperties();
//        param.setTableName("cdc_test");
//        param.setFullTable(true);
//        param.setDetail(true);
//        param.setOperation("insert,delete,update");

        param.setTableName("cdc_test");
        param.setColumnName("name");
        param.setDetail(true);
        param.setOperation("insert,delete,update");

        list.add(param);
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost("http://localhost:8080/api/register");
        System.out.println(JSON.toJSONString(list));
        StringEntity params =new StringEntity(JSON.toJSONString(list));
        request.addHeader("content-type", "application/json");
        request.setEntity(params);
        HttpResponse response = httpClient.execute(request);
        System.out.println("response");
        System.out.println(response);

    }


//    @Test
    public void unRegister() throws Exception {
        List<RegisterProperties> list = new ArrayList<>();
        RegisterProperties param = new RegisterProperties();

        param.setRoutingKey("f1a2538aba1c4ad38f270b0ecfaa55f8");
        param.setTableName("cdc_test");
        param.setFullTable(true);
        list.add(param);
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost("http://localhost:8080/api/unRegister");
        StringEntity params =new StringEntity(JSON.toJSONString(list));
        request.addHeader("content-type", "application/json");
        request.setEntity(params);
        HttpResponse response = httpClient.execute(request);
        System.out.println("response");
        System.out.println(response);

    }
}