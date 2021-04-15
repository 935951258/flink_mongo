package com.event.ingestion.kafka;

import com.event.ingestion.common.HBaseWriter;
import com.event.ingestion.common.Parsable;
import com.event.ingestion.common.Persistable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * UserFriendsConsumer业务代码编写
 * user_id,friend_id
 */
public class UserFriendsConsumer extends BaseConsumer {

    //hbase parser
    public static class UserFriendsHBaseParser implements Parsable<Put> {
        @Override
        public Boolean isValid(String[] fields) {
            return (fields.length>1);
        }

        @Override
        public Boolean isHeader(String[] fields) {
            return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("friend_id"));
        }

        @Override
        public Put parse(String[] fields) {
            Put p = new Put(Bytes.toBytes(fields[0]+"."+fields[1].hashCode()));

            //profile:uf
            p = p.addColumn(Bytes.toBytes("uf"),Bytes.toBytes("user_id"),Bytes.toBytes(fields[0]));
            p = p.addColumn(Bytes.toBytes("uf"),Bytes.toBytes("friend_id"),Bytes.toBytes(fields[1]));
            return p;
        }
    }

    public UserFriendsConsumer() {
        super(new Persistable[]{new HBaseWriter("events_db:user_friend",new UserFriendsHBaseParser())});
    }


    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getKafkaTopic() {
        return "user_friends";
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "grpUserFriends";
    }

    @Override
    protected int getMaxPollRecords() {
        return 32000;
    }
}
