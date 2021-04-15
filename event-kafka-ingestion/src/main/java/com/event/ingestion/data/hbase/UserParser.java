package com.event.ingestion.data.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//hbase parser
public class UserParser extends com.event.ingestion.data.UserParser<Put> {
    //parse the record
    public Put parse(String[] fields) {
        //user_id, locale, birth_year, gender, joined_at, location, time_zone
        //user id
        Put p = new Put(Bytes.toBytes(fields[0]));

        //profile: birth_year
        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("birth_year"), Bytes.toBytes(fields[2]));
        //profile: gender
        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("gender"), Bytes.toBytes(fields[3]));

        //region: locale
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("locale"), Bytes.toBytes(fields[1]));
        //region: location
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("location"), Bytes.toBytes(fields[5]));
        //region: time-zone
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("time_zone"), Bytes.toBytes(fields[6]));

        //registration: joined_at
        p = p.addColumn(Bytes.toBytes("registration"), Bytes.toBytes("joined_at"), Bytes.toBytes(fields[4]));

        //result
        return p;
    }
}