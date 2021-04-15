package com.event.ingestion.kafka;

import com.event.ingestion.common.HBaseWriter;
import com.event.ingestion.common.Parsable;
import com.event.ingestion.common.Persistable;
import com.event.ingestion.kafka.BaseConsumer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * TrainConsumer业务代码编写
 */
public class TrainConsumer extends BaseConsumer {

    public static class TrainHBaseParser implements Parsable<Put> {
        @Override
        public Boolean isValid(String[] fields) {
            return (fields.length>5);
        }

        @Override
        public Boolean isHeader(String[] fields) {
            //user,event,invited,timestamp,interested,not_interested
            return (isValid(fields) && fields[0].equals("user")&& fields[1].equals("event")&& fields[2].equals("invited")&& fields[3].equals("timestamp")&& fields[4].equals("interested")&& fields[5].equals("not_interested"));
        }

        @Override
        public Put parse(String[] fields) {
            //row_key
            Put p = new Put(Bytes.toBytes((fields[0]+"."+fields[1]).hashCode()));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("user"),Bytes.toBytes(fields[0]));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("event"),Bytes.toBytes(fields[1]));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("invited"),Bytes.toBytes(fields[2]));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("timestamp"),Bytes.toBytes(fields[3]));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("interested"),Bytes.toBytes(fields[4]));
            p = p.addColumn(Bytes.toBytes("eu"),Bytes.toBytes("not_interested"),Bytes.toBytes(fields[5]));
            return p;
        }
    }

    public TrainConsumer() {
        super(new Persistable[]{new HBaseWriter("events_db:train",new TrainHBaseParser())});
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getKafkaTopic() {
        return "train";
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "grpTrain";
    }

}
