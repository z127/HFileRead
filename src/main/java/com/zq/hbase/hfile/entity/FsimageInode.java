package com.zq.hbase.hfile.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FsimageInode implements Serializable {
    private String id;
    private  String type;
    private String name;
    private String parentid;
    private Map<String,FsimageInode> childMap;
    private  ArrayList<String> blocksidList;

    public FsimageInode(String id, String type, String name) {
        this.id = id;
        this.type = type;
        this.name = name;
        initChildList();
    }


    public FsimageInode() {
        initChildList();
    }

    public Map<String, FsimageInode> getChildMap() {
        return childMap;
    }

    public void setChildMap(Map<String, FsimageInode> childMap) {
        this.childMap = childMap;
    }

    @Override
    public String toString() {
        return "FsimageInode{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", parentid='" + parentid + '\'' +
                ", childMap=" + childMap +
                ", blocksidList=" + blocksidList +
                '}';
    }

    public String getParentid() {
        return parentid;
    }

    public void setParentid(String parentid) {
        this.parentid = parentid;
    }



    public  void addBlocks(String id)
    {
        blocksidList.add(id);
    }

    public ArrayList<String> getBlocksidList() {
        return blocksidList;
    }

    public void setBlocksidList(ArrayList<String> blocksid) {
        this.blocksidList = blocksid;
    }

    public void initChildList() {
        if (childMap == null)
            childMap = new HashMap<String, FsimageInode>();
        if(blocksidList==null)
            blocksidList=new ArrayList<String>();
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }



    public  void addChild(FsimageInode node){
        this.childMap.put(node.name,node);
    }
}
