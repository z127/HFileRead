package com.zq.hbase.hfile.entity;

import java.util.HashMap;
import java.util.Map;

public class ServerNode {
    private  String ip;
    private  String username;
    private String password;
    private  String Path;
    private Map<String,String> BlockMap;

    public Map<String, String> getBlockMap() {
        return BlockMap;
    }

    public void setBlockMap(Map<String, String> blockMap) {
        BlockMap = blockMap;
    }

    public ServerNode()
    {
        initBlockMap();
    }

    private void initBlockMap() {
        BlockMap=new HashMap<String, String>();
    }

    public ServerNode(String ip, String username, String password, String path) {
        this.ip = ip;
        this.username = username;
        this.password = password;
        Path = path;
        initBlockMap();
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPath() {
        return Path;
    }

    public void setPath(String path) {
        Path = path;
    }

    @Override
    public String toString() {
        return "ServerNode{" +
                "ip='" + ip + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", Path='" + Path + '\'' +
                '}';
    }
}
