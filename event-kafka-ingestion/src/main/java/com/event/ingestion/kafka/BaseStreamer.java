package com.event.ingestion.kafka;

import com.event.ingestion.config.LoadConfig;
import com.event.ingestion.ingestionExecutor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafkaStream数据变换接口定义
 *
 */
public abstract class BaseStreamer implements ingestionExecutor {

    //定义参数常量
    private String zooKeeperUrl = null;
    private String kafkaBrokerUrl = null;

    //数据变换过程中产生的中间数据
    private String stateDir = null;

    //制定程序运行的ID
    protected abstract String getApplicationId();
    //数据源
    protected abstract String getSourceTopic();
    //目的地
    protected abstract String getTargetTopic();

    //初始化配置参数
    public void initialize(Properties properties){
        this.zooKeeperUrl = properties.getProperty(LoadConfig.zooKeeperUrl);
        this.kafkaBrokerUrl = properties.getProperty(LoadConfig.kafkaBrokerUrl);
        this.stateDir = properties.getProperty(LoadConfig.stateDir);
    }

    //定义子类的实现的数据业务拆分方法
    //(123123,[123123,123123])
    //([123123,123123])
    protected abstract List<String[]> transfrom(String[] fields);
    //子类校验头信息
    protected abstract Boolean isHeader(String[] fields);

    //子类校验记录是否有效
    protected abstract Boolean isValid(String[] fields);

    //业务代码入口
    protected void stream() throws Exception{
        //定义Properties封装初始化参数
        Properties properties = new Properties();

        //app_id
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,getApplicationId());
        //kafka地址
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBrokerUrl);
        //zookeeper地址
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zooKeeperUrl);
        //key-value的序列化
        //Serdes.String().getClass().getName() 获取全类名
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //设置state目录存放kafkaStream传输的状态信息
        properties.put(StreamsConfig.STATE_DIR_CONFIG,stateDir);

        //创建KStreamBuilder对象
        KStreamBuilder kStreamBuilder =new KStreamBuilder();
        //绑定sourceTopic
        KStream<String,String> stream = kStreamBuilder.stream(Serdes.String(),Serdes.String(),this.getSourceTopic());
        //数据变换
        KStream<String, String> result = stream.flatMap((k, v) -> transfrom(k, v)).filter((k, v) -> v != null && v.length() > 0);
        result.to(Serdes.String(),Serdes.String(),this.getTargetTopic());

        //启动数据传输进程
        StreamsConfig config = new StreamsConfig(properties);
        (new KafkaStreams(kStreamBuilder,config)).start();
    }

    //数据转换方法
    private Iterable<KeyValue<String,String>> transfrom(String key ,String value){

        List<KeyValue<String,String>> items=new ArrayList<KeyValue<String, String>>();

        //先进性过打散 （1，1，1，1，1）
        //对value进行切割
        String[] fileds = value.split(",", -1);
        if (isHeader(fileds) || !isValid(fileds)){

            //不满足条件的用空 填充
            items.add(new KeyValue<>(key,""));
        }else {
            for (String[] vs:transfrom(fileds)){

                items.add(new KeyValue<>(key,String.join(",",vs)));
            }
        }
            return items;
    }

    //程序入口


    @Override
    public void execute(String[] args) throws Exception {

        if(args.length <1){
            System.out.println("参数异常");
        }else {

            try {
                this.initialize(LoadConfig.loadSettings(args[0]));
                this.stream();
            }catch (Exception e){
                e.printStackTrace();
            }

        }

    }
}
