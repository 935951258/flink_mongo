package com.event.ingestion.data.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//hbase parser
public class EventParser extends com.event.ingestion.data.EventParser<Put> {
    //parse the record
    public Put parse(String[] fields) {
        //event id
        Put p = new Put(Bytes.toBytes(fields[0]));

        //schedule: start_time
        p = p.addColumn(Bytes.toBytes("schedule"), Bytes.toBytes("start_time"), Bytes.toBytes(fields[2]));

        //location: city
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes(fields[3]));
        //location: state
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("state"), Bytes.toBytes(fields[4]));
        //location: zip
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("zip"), Bytes.toBytes(fields[5]));
        //location: country
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("country"), Bytes.toBytes(fields[6]));
        //location: lat
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lat"), Bytes.toBytes(fields[7]));
        //location: lng
        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lng"), Bytes.toBytes(fields[8]));
        //user id
        p = p.addColumn(Bytes.toBytes("creator"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[1]));

        //remark
        StringBuffer sb = new StringBuffer();
        //check
        if ( fields.length > 8 ) {
            //check
            for ( int i = 9; i < fields.length; i++ ) {
                //check
                if ( sb.length() > 0 )
                    sb.append( "|" );
                //append
                sb.append( fields[i] );
            }
        }
        //remark
        p = p.addColumn(Bytes.toBytes("remark"), Bytes.toBytes("common_words"), Bytes.toBytes(sb.toString()));

        //creator: user_id
        return p;
    }
}