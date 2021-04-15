package com.event.ingestion.data.mongo;

import com.event.ingestion.common.Tuple;
import com.mongodb.BasicDBObject;


//mongo parser
public class EventParser extends com.event.ingestion.data.EventParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        //the key
        BasicDBObject k = new BasicDBObject();
        //event id
        k.put("event_id", fields[0]);

        //the doc
        BasicDBObject d = new BasicDBObject();
        //event id
        d.put("event_id", fields[0]);

        //schedule
        BasicDBObject schedule = new BasicDBObject();
        //start_time
        schedule.put("start_time", fields[2]);
        //add schedule
        d.put("schedule", schedule);

        //location
        BasicDBObject location = new BasicDBObject();
        //city
        location.put("city", fields[3]);
        //state
        location.put("state", fields[4]);
        //zip
        location.put("zip", fields[5]);
        //country
        location.put("country", fields[6]);
        //lat
        location.put("lat", fields[7]);
        //lng
        location.put("lng", fields[8]);
        //add location
        d.put("location", location);

        //creator
        BasicDBObject creator = new BasicDBObject();
        //user id
        creator.put("user_id", fields[1]);
        //add the creator
        d.put("creator", creator);

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
        BasicDBObject remark = new BasicDBObject();
        //common_words
        remark.put("common_words", sb.toString());
        //add remark
        d.put("remark", remark);

        //result
        return new Tuple<>(k, d);
    }
}

