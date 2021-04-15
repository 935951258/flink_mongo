package com.event.ingestion.kafka;

import com.event.ingestion.common.HBaseWriter;
import com.event.ingestion.common.Parsable;
import com.event.ingestion.common.Persistable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * EventAttendeesConsumer业务代码编写
 */
public class EventAttendeesConsumer extends BaseConsumer {

    public static class EventAttendeesHBaseParser implements Parsable<Put> {
        //event_id,user_id,attend_type
        @Override
        public Boolean isHeader(String[] fields) {
            return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("attend_type"));
        }

        @Override
        public Boolean isValid(String[] fields) {
            return (fields.length >2);
        }

        @Override
        public Put parse(String[] fields) {
            Put p = new Put(Bytes.toBytes((fields[0]+"."+fields[1]+"-"+fields[2]).hashCode()));//row-key
            p = p.addColumn(Bytes.toBytes("euat"),Bytes.toBytes("event_id"),Bytes.toBytes(fields[0]));
            p = p.addColumn(Bytes.toBytes("euat"),Bytes.toBytes("user_id"),Bytes.toBytes(fields[1]));
            p = p.addColumn(Bytes.toBytes("euat"),Bytes.toBytes("attend_type"),Bytes.toBytes(fields[2]));
            return p;
        }
    }

    public EventAttendeesConsumer() {
        super(new Persistable[]{new HBaseWriter("events_db:event_attendees",new EventAttendeesHBaseParser())});
    }

    @Override
    protected int getMaxPollRecords() {
        return 32000;
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "grpEventAttendees2";
    }

    @Override
    protected String getKafkaTopic() {
        return "event_attendees";
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return true;
    }
}
