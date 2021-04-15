package com.event.ingestion.common;

import org.apache.commons.lang.ArrayUtils;

public interface Parsable<T> {

    Boolean isHeader(String[] fields);
    Boolean isValid(String[] fields);

    //check if a record is empty
    default Boolean isEmpty(String[] fields, int[] indexes) {
        Boolean empty = false;
        //check
        if ( fields != null && fields.length > 0 ) {
            //loop
            for ( int i = 0; i < fields.length; i++ ) {
                //check
                if ( indexes != null && ArrayUtils.contains(indexes,  i) ) {
                    //combine
                    empty |= (fields[i] == null || fields[i].trim().length() <= 0);
                }
            }
        }
        return empty;
    }

    //数据变换
    T parse(String[] fields);

}
