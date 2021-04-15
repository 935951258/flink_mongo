package com.event.ingestion.data.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//hbase parser
public class EventAttendeesParser extends com.event.ingestion.data.EventAttendeesParser<Put> {
    //parse the record
    public Put parse(String[] fields) {
        //create - Row-Key
        Put p = new Put(Bytes.toBytes(fields[0] + "." + fields[1] + "-" + fields[2]));

        //euat: event_id
        p = p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("event_id"), Bytes.toBytes(fields[0]));
        //euat: user_id
        p = p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[1]));
        //euat: attend_type
        p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("attend_type"), Bytes.toBytes(fields[2]));

        //the result
        return p;
    }
}