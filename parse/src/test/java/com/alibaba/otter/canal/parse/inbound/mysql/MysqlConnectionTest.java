package com.alibaba.otter.canal.parse.inbound.mysql;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class MysqlConnectionTest {

    private static Logger log = LoggerFactory.getLogger(MysqlConnectionTest.class);

    private MysqlConnection connection;

    @Before
    public void before() throws IOException {
        connection = new MysqlConnection(new InetSocketAddress("127.0.0.1",3306),"canal","canal");
        connection.connect();
        log.info("获取连接成功：{}",connection);
    }

    @Test
    public void testQuery() throws IOException {
        ResultSetPacket resultSetPacket = connection.query("show variables like '%'");
        log.info("结果："+resultSetPacket);
    }

    @Test
    public void testQuery2() throws IOException {
        ResultSetPacket resultSetPacket = connection.query("select * from lilishop.li_store");
        List<FieldPacket> fieldDescriptors = resultSetPacket.getFieldDescriptors();
        List<String> fieldValues = resultSetPacket.getFieldValues();

        int fieldSize = fieldDescriptors.size();
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<fieldValues.size();i++){
            int index = i%fieldSize;
            if(i!=0 && index==0){
                log.info(sb.toString());
                sb = new StringBuffer();
            }
            FieldPacket field = fieldDescriptors.get(index);
            sb.append(field.getName()).append("=").append(fieldValues.get(i)).append(",");
        }
        System.out.println(resultSetPacket);
    }
}