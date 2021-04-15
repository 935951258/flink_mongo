package com.event.ingestion.kafka;

import com.event.ingestion.common.Persistable;
import com.event.ingestion.config.LoadConfig;
import com.event.ingestion.ingestionExecutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class BaseConsumer implements ingestionExecutor {

    //kafka broker url
    private String kafkaBrokerUrl = null;
    //消费者的topic
    protected abstract String getKafkaTopic();
    //手动控制commit
    protected abstract Boolean getKafkaAutoCommit();
    //每次从topic读取的最大数
    protected int getMaxPollRecords(){
        return 6400;
    }

    //指定consumer消费组
    protected abstract String getKafkaConsumerGrp();

    //writes
    private Persistable[] writes = null;

    //constructor
    //继承BaseConsumer就要从写该构造方法，并传入 Persistable[] writes
    public BaseConsumer(Persistable[] writes){
        this.writes =writes;
    }

    //该初始化方法是第一次 初始化配置
    public void initialize(Properties properties){
        this.kafkaBrokerUrl = properties.getProperty(LoadConfig.kafkaBrokerUrl);
        //校验
        if (this.writes != null && this.writes.length > 0){
            for (Persistable write : writes) {
                write.initalize(properties);
            }
        }

    }

    //consumer
    protected void consume() throws Exception{
        if (this.kafkaBrokerUrl == null || this.kafkaBrokerUrl.isEmpty()){
            throw new Exception("kafka broker url is not initialize");
        }
        //create properties for consumer
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,this.kafkaBrokerUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.getKafkaConsumerGrp());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.getKafkaAutoCommit() ?"true":"false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,"180000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"120000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,Integer.toString(this.getMaxPollRecords()));//每次拿多少数据
        //key value序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //topic
        List<TopicPartition> topics = Arrays.asList(new TopicPartition(getKafkaTopic(),0));

        consumer.subscribe(Arrays.asList(new String[]{getKafkaTopic()})); //能负载均衡
        /*consumer.assign(topics);
        consumer.seek(topics.get(0),0L);*/

        //写数据
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(3000L);

                int recordsCount = (records != null) ? records.count():0;

                //check
                if (recordsCount <= 0){
                    //没有拿到数据 等待三秒
                    Thread.sleep(3);
                    continue;

                }
                System.out.println("messages polled...."+ recordsCount);

                //check
                if (this.writes != null && this.writes.length > 0){
                    for (Persistable write : writes) {
                        write.write(records);
                    }
                    //check
                    if (!this.getKafkaAutoCommit()){
                        //异步提交
                        consumer.commitSync();
                    }
                }
                System.out.println("写完了");
            }
        }finally {
            consumer.close();
        }

    }

    //程序入口
    @Override
    public void execute(String[] args) throws Exception {
        if (args.length<1){
            System.out.println("参数异常");
        }else {
            this.initialize(LoadConfig.loadSettings(args[0]));
            this.consume();
        }
    }
}
