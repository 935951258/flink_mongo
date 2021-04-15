package com.event.ingestion.data.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//hbase parser
public class TrainParser extends com.event.ingestion.data.TrainParser<Put> {
    //parse the record
    public Put parse(String[] fields) {
        //key
        Put p = new Put(Bytes.toBytes(fields[0] + "." + fields[1]));  //row-key

        //eu: user
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("user"), Bytes.toBytes(fields[0]));
        //eu:event
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("event"), Bytes.toBytes(fields[1]));

        //eu: invited
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("invited"), Bytes.toBytes(fields[2]));
        //eu:time_stamp
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("time_stamp"), Bytes.toBytes(fields[3]));
        //eu: interested
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("interested"), Bytes.toBytes(fields[4]));
        //eu: not_interested
        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("not_interested"), Bytes.toBytes(fields[5]));

        //result
        return p;
    }
}