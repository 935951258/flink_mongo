package com.event.ingestion.data.mongo;

import com.event.ingestion.common.Tuple;
import com.mongodb.BasicDBObject;


//mongo parser
public class EventAttendeesParser extends com.event.ingestion.data.EventAttendeesParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        BasicDBObject d = new BasicDBObject();

        //event_id
        d.put("event_id", fields[0]);
        //user_id
        d.put("user_id", fields[1]);
        //attend_type
        d.put("attend_type", fields[2]);

        //the key & doc are the same
        return new Tuple<>(d, d);
    }
}

