package com.zq.hbase.hfile.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TableNode implements Serializable {
    private   String tableName;
    private String namespace;
    private List<String> blocksIdList;

    public TableNode(String table, String namespace) {
        this.tableName = table;
        this.namespace = namespace;
        initList();
    }





    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getBlocksIdList() {
        return blocksIdList;
    }

    public void setBlocksIdList(List<String> blocksIdList) {
        this.blocksIdList = blocksIdList;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }






    public void initList() {
        if (blocksIdList == null)
            blocksIdList = new ArrayList<String>();
    }

    public  void addblock(String block)
    {
        blocksIdList.add(block);
    }
}
