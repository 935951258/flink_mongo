package com.event.ingestion.kafka.streamer;

import com.event.ingestion.kafka.BaseStreamer;

import java.util.ArrayList;
import java.util.List;

public class EventAttendeesStreamer extends BaseStreamer {
    @Override
    protected String getApplicationId() {
        return "event-attendees-streamming";
    }

    @Override
    protected String getSourceTopic() {
        return "event_attendees_raw";
    }

    @Override
    protected String getTargetTopic() {
        return "event_attendees";
    }

    @Override
    protected List<String[]> transfrom(String[] fields) {

        List<String[]>results=new ArrayList<>();

        //event_id
        String event_id=fields[0];
        //status=> yes
        if (fields.length >1 &&fields[1]!=null){
            String[] yesUser = fields[1].split(" ");

            if (yesUser !=null && yesUser.length>0){
                for (String yesNub : yesUser) {
                    results.add(new String[]{event_id,yesNub,"yes"});
                }
            }

        }

        //status=> maybe
        if (fields.length>2 && fields[2] !=null){
            //split
            String[] maybeUser = fields[2].split(" ");
            if (maybeUser !=null &&maybeUser.length >0){
                for (String maybyNub : maybeUser) {

                    results.add(new String[]{event_id,maybyNub,"maybe"});
                }
            }
        }

        //status=>invute
        if (fields.length>3 && fields[3] != null){
            String[] invetedUser = fields[3].split(" ");
            if (invetedUser !=null &&invetedUser.length >0){
                for (String invitedNub : invetedUser) {
                    results.add(new String[]{event_id,invitedNub,"invited"});

                }
            }
        }

        //status=>no
        if (fields.length> 4 && fields[4] !=null){
            String[] noUser = fields[4].split(" ");
            if (noUser !=null && noUser.length>0){

                for (String noNub : noUser) {
                    results.add(new String[]{event_id,noNub,"no"});
                }
            }
        }

        return results;
    }

    @Override
    protected Boolean isHeader(String[] fields) {
        return (isValid(fields) && fields[0].equals("event") && fields[1].equals("yes") && fields[2].equals("maybe") && fields[3].equals("invited") && fields[4].equals("no"));
    }

    @Override
    protected Boolean isValid(String[] fields) {
        return (fields.length == 5);
    }
}
