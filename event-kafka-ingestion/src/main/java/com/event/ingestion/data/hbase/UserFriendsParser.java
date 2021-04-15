package com.event.ingestion.data.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//hbase parser
public class UserFriendsParser extends com.event.ingestion.data.UserFriendsParser<Put> {
    //parse the record
    public Put parse(String[] fields) {
        //user id
        Put p = new Put(Bytes.toBytes( fields[0] + "-" + fields[1] ));

        //profile: uf
        p = p.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[0]));
        //profile: uf
        p = p.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("friend_id"), Bytes.toBytes(fields[1]));

        //result
        return p;
    }
}
