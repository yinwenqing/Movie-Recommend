package com.ywq.server.model.core;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Tag {
    @JsonIgnore
    private int id;

    private int uid;

    private int mid;

    private String tag;

    private long timestamp;

    public Tag(int uid, int mid, String tag, long timestamp) {
        this.uid = uid;
        this.mid = mid;
        this.tag = tag;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
