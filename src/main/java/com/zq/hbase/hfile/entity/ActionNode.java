package com.zq.hbase.hfile.entity;

public class ActionNode {
String vlen;
String row;
String family;
String qualifier;
String timestamp;

    @Override
    public String toString() {
        return "ActionNode{" +
                "vlen='" + vlen + '\'' +
                ", row='" + row + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }

    public String getVlen() {
        return vlen;
    }

    public void setVlen(String vlen) {
        this.vlen = vlen;
    }

    public String getRow() {
        return row;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
