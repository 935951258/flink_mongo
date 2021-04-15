package com.event.ingestion.kafka;

import com.event.ingestion.config.LoadConfig;
import com.event.ingestion.ingestionExecutor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 生产者业务代码
 */
public class EventProducer implements ingestionExecutor {
    @Override
    public void execute(String[] args) throws Exception {
        if (args.length != 3){
            System.out.println("参数不匹配");
        }else {
            //获取brokerURL
            String brokerurl = LoadConfig.loadSettings(args[0]).getProperty(LoadConfig.kafkaBrokerUrl);
            //指定topicName
            String topic = args[1].toString();
            //指定要上传的文件
            String filename = args[2].toString();

            //创建properties对象
            Properties properties = new Properties();
            //设置服务器url
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerurl);
            //properties.put("bootstrap.servers",brokerurl);
            //设置acks等级
            properties.put(ProducerConfig.ACKS_CONFIG,"1");
            //properties.put("acks","1");
            //如果发送失败，自动发送的次数
            properties.put(ProducerConfig.RETRIES_CONFIG,1);
            //properties.put("retries",1);
            //当多个消息发送到相同的分区时，生产者尝试将消息批量打包在一起，以减少请求交互，这样可以提高性能
            //批次大小
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);//16kd
            //properties.put("batch.size",16384);//16kd
            //假设batch时32k，那么你得估算一下，正常情况下，需要多长时间凑够一个batch批次；
            //当batch未满16k 但是到了以毫秒，则会强制发送
            properties.put(ProducerConfig.LINGER_MS_CONFIG,5);
            //properties.put("linger.ms",5);
            //生产者用来缓存等待发送到服务器的消息的总字节数
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
            //properties.put("buffer.memory",33554432);
            //key和value的序列化
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
            //properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
            //properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

            //实例化 producer
            //创建生产者对象
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            try{

                //创建字符输入流
                BufferedReader reader = new BufferedReader(new FileReader(filename));
                try {

                    //定义 一行长度的变量，和总数变量
                    long key =0,count=0;
                    String line = null;
                    //String line = reader.readLine();

                    //循环读取
                    while ((line=reader.readLine()) != null){
                        key += line.length() + 1;

                        //send data
                        producer.send(new ProducerRecord<String, String>(topic,Long.toString(key),line));
                        count++;
                        //line = reader.readLine();
                    }

                    //打印总数
                    System.out.println("总数为:"+count);
                }finally {

                    //关闭字符流
                    reader.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {

                //关闭生产者对象
                producer.close();
            }

        }

    }
}
