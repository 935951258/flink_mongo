package com.event.ingestion.data;

import com.event.ingestion.common.Parsable;


public abstract class EventParser<T> implements Parsable<T> {
    //check if the record is a header
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("start_time")
                && fields[3].equals("city") && fields[4].equals("state") && fields[5].equals("zip") && fields[6].equals("country")
                && fields[7].equals("lat") && fields[8].equals("lng"));
    }

    //check if a record is valid
    public Boolean isValid(String[] fields) {
        //check
        return (fields.length > 8 && !isEmpty(fields, new int[] { 0 }));
    }
}
