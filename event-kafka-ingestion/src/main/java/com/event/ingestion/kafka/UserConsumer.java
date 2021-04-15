package com.event.ingestion.kafka;

import com.event.ingestion.common.HBaseWriter;
import com.event.ingestion.common.Parsable;
import com.event.ingestion.common.Persistable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * usserconsume 业务代码
 */

//从写了父类的4个方法
public class UserConsumer extends BaseConsumer{

    /**
     * public BaseConsumer(Persistable[] writes){
     *         this.writes =writes;
     *     }
     */
    public UserConsumer() {
        //传入new Persistable[] 即初始化了 BaseConsumer(Persistable[] writes)
        //UserHBaseParse() 实现了 Parsable<Put>

        super(new Persistable[]{new HBaseWriter("events_db:users",new UserHBaseParse())});
    }

    @Override
    protected String getKafkaTopic() {
        return "users";
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "grpUsers3";
    }

    //实现了 Parsable<Put>接口  重写了 isHeader  isValid  parse  方法 ，即便给 new UserHBaseParse() 对象 实现 Parsable<Put> 给 new HBaseWriter 调用
    public static class UserHBaseParse implements Parsable<Put>{

        @Override
        public Boolean isHeader(String[] fields) {
            return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale") && fields[2].equals("birthyear") && fields[3].equals("gender") && fields[4].equals("joineAt") && fields[5].equals("location") && fields[6].equals("timezone"));
        }

        @Override
        public Boolean isValid(String[] fields) {
            return (fields.length>6);
        }

        @Override
        public Put parse(String[] fields) {

            //rowkey
            Put put = new Put(Bytes.toBytes(fields[0]));

            //profile
            put.addColumn(Bytes.toBytes("profile"),Bytes.toBytes("birth_year"),Bytes.toBytes(fields[2]));
            put.addColumn(Bytes.toBytes("profile"),Bytes.toBytes("gender"),Bytes.toBytes(fields[3]));

            //region
            put.addColumn(Bytes.toBytes("region"),Bytes.toBytes("locale"),Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("region"),Bytes.toBytes("location"),Bytes.toBytes(fields[5]));
            put.addColumn(Bytes.toBytes("region"),Bytes.toBytes("time_zone"),Bytes.toBytes(fields[6]));

            //registraction
            put.addColumn(Bytes.toBytes("registration"),Bytes.toBytes("joined_at"),Bytes.toBytes(fields[4]));
            return put;
        }
    }


}
