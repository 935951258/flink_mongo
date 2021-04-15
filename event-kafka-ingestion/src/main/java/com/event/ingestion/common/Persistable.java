package com.event.ingestion.common;


import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

/**
 * 数据写入Hbase
 */
public interface Persistable {
    //初始化
    void initalize(Properties properties);
    int write(ConsumerRecords<String,String> records) throws Exception;

}
