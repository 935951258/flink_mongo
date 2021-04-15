package com.event.ingestion.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 *
 * 读取配置文件
 */
public class LoadConfig {

    //zookeeper URL
    public final static String zooKeeperUrl = "zookeeperUrl";

    //broker url
    public final static String kafkaBrokerUrl = "brokerUrl";

    //stateDir
    public final static String stateDir = "stateDir";
    public final static String coreSite = "coreSite";
    public final static String hdfsSite = "hdfsSite";
    public final static String hbaseSite = "hbaseSite";

    //the host of the mongo server
    public final static String mongoHost = "mongoHost";
    //the port of the mongo server for client connection
    public final static String mongoPort = "mongoPort";
    //the target mongo database
    public final static String mongoDatabase = "mongoDatabase";
    //the user used for connecting to mongo
    public final static String mongoUser = "mongoUser";
    //password
    public final static String mongoPwd = "mongoPwd";

    //db jdbc-url
    public final static String dbJdbcUrl = "dbJdbcUrl";
    //user for connecting to db
    public final static String dbUser = "dbUser";
    //user's password
    public final static String dbPassword = "dbPassword";


    //加载文件内容到Properties
    public static Properties loadSettings(String settingsFile) throws Exception {

        //创建生产者配置对象
        Properties properties = new Properties();

        //创建字节输入流
        FileInputStream in = new FileInputStream(settingsFile);

        try {

            //讲字节输入流通过转换流转换成字符流，以便于读取一行
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));

            try {
                //设置一行空变量
                String line = null;
                //String line = bufferedReader.readLine();

                //循环读取一行一行配置信息
                while ((line = bufferedReader.readLine()) != null) {

                    //通过 = 切割，将一行配置信息 分成两个部分
                    String[] values = line.split("=", -1);

                    //如果values长度为2，则封装给配置对象
                    if (values != null && values.length == 2) {
                        properties.put(values[0], values[1]);
                    }
                    //line = bufferedReader.readLine();
                }

            } finally {
                //关闭字符流
                bufferedReader.close();
            }
        } finally {
            //关闭字节流
            in.close();
        }

        //返回配置对象，等待调用
        return properties;


    }
}
