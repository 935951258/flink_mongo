package com.event.ingestion.data.mongo;

import com.event.ingestion.common.Tuple;
import com.mongodb.BasicDBObject;


//mongo parser
public class UserParser extends com.event.ingestion.data.UserParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        //the key
        BasicDBObject k = new BasicDBObject();
        //user id
        k.put("user_id", fields[0]);

        //the doc
        BasicDBObject d = new BasicDBObject();
        //user id
        d.put("user_id", fields[0]);

        //profile
        BasicDBObject profile = new BasicDBObject();
        //birth_year
        profile.put("birth_year", fields[2]);
        //gender
        profile.put("gender", fields[3]);
        //add profile
        d.put("profile", profile);

        //region
        BasicDBObject region = new BasicDBObject();
        //locale
        region.put("locale", fields[1]);
        //location
        region.put("location", fields[5]);
        //time-zone
        region.put("time_zone", fields[6]);
        //add region
        d.put("region", region);

        //registration
        BasicDBObject registration = new BasicDBObject();
        //joined_at
        registration.put("joined_at", fields[4]);
        //add registration
        d.put("registration", registration);

        //result
        return new Tuple<>(k, d);
    }
}

