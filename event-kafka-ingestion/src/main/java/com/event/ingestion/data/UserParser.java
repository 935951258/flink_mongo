package com.event.ingestion.data;

import com.event.ingestion.common.Parsable;


public abstract class UserParser<T> implements Parsable<T> {
    //check if the record is a header
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale") && fields[2].equals("birthyear")
                && fields[3].equals("gender") && fields[4].equals("joinedAt") && fields[5].equals("location") && fields[6].equals("timezone"));
    }

    //check if a record is valid
    public Boolean isValid(String[] fields) {
        //check
        return (fields.length > 6 && !isEmpty(fields, new int[] { 0 }));
    }
}
