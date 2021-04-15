package com.event.ingestion.data;

import com.event.ingestion.common.Parsable;

public abstract class TrainParser<T> implements Parsable<T> {
    //check if the record is a header
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("user") && fields[1].equals("event") && fields[2].equals("invited")
                && fields[3].equals("timestamp") && fields[4].equals("interested") && fields[5].equals("not_interested"));
    }

    //check if a record is valid
    public Boolean isValid(String[] fields) {
        //check
        return (fields.length > 5 && !isEmpty(fields, new int[] { 0, 1 }));
    }
}
