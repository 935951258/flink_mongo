package com.event.ingestion.kafka.streamer;

import com.event.ingestion.kafka.BaseStreamer;

import java.util.ArrayList;
import java.util.List;

public class UserFriendsStreamer extends BaseStreamer {
    @Override
    protected String getApplicationId() {
        return "user-friends-streamming";
    }

    @Override
    protected String getSourceTopic() {
        return "user_friends_raw";
    }

    @Override
    protected String getTargetTopic() {
        return "user_friends";
    }

    //数据变化代码实现
    @Override
    protected List<String[]> transfrom(String[] fields) {
        ArrayList<String[]> result = new ArrayList<>();
        // fields [123,123 123 123 123]
        //结果为 [123,123][123,123]
        String user_id=fields[0];

        //friends
        String[] friends = fields[1].split(" ");
        //check
        if (friends != null && friends.length > 0){

            for (String friend_id : friends) {
                result.add(new String[] {user_id,friend_id});

            }
        }

        return result;
    }

    @Override
    protected Boolean isHeader(String[] fields) {
        //check

        return isValid(fields)&& fields[0].equals("user") && fields[1].equals("friends");
    }

    @Override
    protected Boolean isValid(String[] fields) {

        return fields.length > 1;
    }
}
