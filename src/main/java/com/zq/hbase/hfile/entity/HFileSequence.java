package com.zq.hbase.hfile.entity;

import java.util.List;

public class HFileSequence {
    String sequence;
    String region;
    List<ActionNode> actions;
    HFileTableNode table;

    public HFileTableNode getTable() {
        return table;
    }

    public void setTable(HFileTableNode table) {
        this.table = table;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }


    public List<ActionNode> getActions() {
        return actions;
    }

    public void setActions(List<ActionNode> actions) {
        this.actions = actions;
    }

    @Override
    public String toString() {
        return "HFileSequence{" +
                "sequence='" + sequence + '\'' +
                ", region='" + region + '\'' +
                ", actions=" + actions +
                ", table=" + table +
                '}';
    }
}
