package com.event.ingestion.kafka;

import com.event.ingestion.common.HBaseWriter;
import com.event.ingestion.common.Parsable;
import com.event.ingestion.common.Persistable;
import com.event.ingestion.kafka.BaseConsumer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * EventConsumer业务代码编写
 */
public class EventConsumer extends BaseConsumer {
    public static class EventHBaseParser implements Parsable<Put> {
        @Override
        public Boolean isValid(String[] fields) {
            return (fields.length > 8);
        }

        //event_id,user_id,start_time,city,state,zip,country,lat,lng,
        @Override
        public Boolean isHeader(String[] fields) {
            return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("start_time")
                    && fields[3].equals("city") && fields[4].equals("state") && fields[5].equals("zip")
                    && fields[6].equals("country") && fields[7].equals("lat") && fields[8].equals("lng"));
        }


        @Override
        public Put parse(String[] fields) {
            Put p = new Put(Bytes.toBytes(fields[0]));

            //event_id,user_id,start_time,city,state,zip,country,lat,lng,
            //schedule：start_time
            p = p.addColumn(Bytes.toBytes("schedule"),Bytes.toBytes("start_time"),Bytes.toBytes(fields[2]));

            //location:city
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("city"),Bytes.toBytes(fields[3]));
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("state"),Bytes.toBytes(fields[4]));
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("zip"),Bytes.toBytes(fields[5]));
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("country"),Bytes.toBytes(fields[6]));
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("lat"),Bytes.toBytes(fields[7]));
            p = p.addColumn(Bytes.toBytes("location"),Bytes.toBytes("lng"),Bytes.toBytes(fields[8]));

            //creator
            p = p.addColumn(Bytes.toBytes("creator"),Bytes.toBytes("user_id"),Bytes.toBytes(fields[1]));

            StringBuilder sb = new StringBuilder();
            if (fields.length > 8) {
                for (int i = 9;i<fields.length;i++) {
                    if(sb.length() > 0) {
                        sb.append("|");
                    }
                    sb.append(fields[i]);
                }
            }
            //remark
            p.addColumn(Bytes.toBytes("remark"),Bytes.toBytes("common_words"),Bytes.toBytes(sb.toString()));
            return p;
        }
    }

    public EventConsumer() {
        super(new Persistable[]{new HBaseWriter("events_db:events",new EventHBaseParser())});
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return  "grpEvents";
    }

    @Override
    protected String getKafkaTopic() {
        return "events";
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return true;
    }

    @Override
    protected int getMaxPollRecords() {
        return 32000;
    }
}
