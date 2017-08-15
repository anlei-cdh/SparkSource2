package com.java.util;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @function:kafka记录类
 */
public class KafkaTopicOffset implements Serializable {
    private String topicName;
    private HashMap<Integer, Long> offsetList;
    private HashMap<Integer, String> leaderList;

    public KafkaTopicOffset(String topicName) {
        this.topicName = topicName;
        this.offsetList = new HashMap<Integer, Long>();
        this.leaderList = new HashMap<Integer, String>();
    }

    public String getTopicName() {
        return topicName;
    }

    public HashMap<Integer, Long> getOffsetList() {
        return offsetList;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setOffsetList(HashMap<Integer, Long> offsetList) {
        this.offsetList = offsetList;
    }

    public HashMap<Integer, String> getLeaderList() {
        return leaderList;
    }

    public void setLeaderList(HashMap<Integer, String> leaderList) {
        this.leaderList = leaderList;
    }

    public String toString() {
        return "topic:" + topicName + ",offsetList:" + this.offsetList + ",leaderList:" + this.leaderList;
    }
}  
