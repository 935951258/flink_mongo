package com.event.ingestion.data;

import com.event.ingestion.common.Parsable;


public abstract class EventAttendeesParser<T> implements Parsable<T> {
    //check if the record is a header
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("attend_type"));
    }

    //check if a record is valid
    public Boolean isValid(String[] fields) {
        //check - evnet_id, yes, maybe, invited, no
        return (fields.length > 2 && !isEmpty(fields, new int[] { 0, 1, 2 }));
    }
}
