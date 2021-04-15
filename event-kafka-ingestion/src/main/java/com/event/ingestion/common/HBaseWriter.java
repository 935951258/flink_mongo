package com.event.ingestion.common;

import com.event.ingestion.config.LoadConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Properties;

public class HBaseWriter implements Persistable{

    //core-site.xml
    private String coreSite = null;
    //hdfs-site.xml
    private String hdfsSite = null;
    //hbase-site.xml
    private String hbaseSite = null;

    //指定table
    private String hbTable = null;

    //解析数据
    private Parsable<Put> parse = null;

    //constructor
    public HBaseWriter(String hbTable, Parsable<Put> parser){
        this.hbTable = hbTable;
        this.parse = parser;
    }

    //初始化 configuration
    @Override
    public void initalize(Properties properties) {
        this.coreSite = properties.getProperty(LoadConfig.coreSite);
        this.hbaseSite = properties.getProperty(LoadConfig.hbaseSite);
        this.hdfsSite = properties.getProperty(LoadConfig.hdfsSite);
    }

    @Override
    public int write(ConsumerRecords<String, String> records) throws Exception {

        //写入hbase的数据
        int numPuts = 0;
        if(this.hbaseSite == null || this.hbaseSite.isEmpty()){
            throw new Exception("hbase-site.xml 没有被初始化");
        }

        //参数设置
        Configuration configuration = HBaseConfiguration.create();
        if(this.coreSite != null){
            configuration.addResource(new Path(this.coreSite));
        }
        if (this.hdfsSite != null){
            configuration.addResource(new Path(this.hdfsSite));
        }
        configuration.addResource(new Path(this.hbaseSite));

        //create connection
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            //获取
            Table table = connection.getTable(TableName.valueOf(this.hbTable));
            try {
                ArrayList<Put> puts = new ArrayList<>();
                //flags
                long passHead =0L;
                for(ConsumerRecord<String,String> record :records){
                    //解析数据
                    String[] elements = record.value().split(",",-1);
                    //判断第一行，过滤掉
                    if (passHead == 0L && (this.parse.isHeader(elements))){
                        passHead = 1L;
                        //continue;
                    }

                    if (this.parse.isValid(elements)){
                        //数据绑定成put对象
                        puts.add(this.parse.parse(elements));
                    }
                }
                if(puts.size()>0){
                    //保存数据到hbase
                    table.put(puts);
                }
                numPuts = puts.size();

            }finally {
                table.close();
            }

        }finally {
            connection.close();

        }

        return numPuts;
    }
}
