package com.zq.hbase.hfile.xmlparser;

import com.zq.hbase.hfile.entity.ServerNode;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

public class ParseConfigurationFile {
    public static void main(String[] args) throws IOException {
      ArrayList<ServerNode> list= ParseConfiuration("D:\\HbaseData\\configuration");
      //生成block和服务器的映射
      GenerateMapList(list);
    }

    /**
     * 生成keyvalue对
     * @param list
     */
    private static void GenerateMapList(ArrayList<ServerNode> list) {
        for(int i=0;i<list.size();i++)
        {
            traverseFolder(list.get(i).getPath(),list.get(i));
        }
        
    }

    /**
     * 遍历文件夹
     * @param path
     * @param node
     */
    public static void traverseFolder(String path,ServerNode node) {
        File file = new File(path);
        if (file.exists()) {
            LinkedList<File> list = new LinkedList<File>();
            File[] files = file.listFiles();
            for (File file2 : files) {
                if (file2.isDirectory()) {
                   // System.out.println("文件夹:" + file2.getAbsolutePath());
                    list.add(file2);
                } else {
                    PutIntoMap(node, file2);
                }
            }
            File temp_file;
            while (!list.isEmpty()) {
                temp_file = list.removeFirst();
                files = temp_file.listFiles();
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                       // System.out.println("文件夹:" + file2.getAbsolutePath());
                        list.add(file2);
                    } else {
                        PutIntoMap(node, file2);
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }

        System.out.println(node.getBlockMap().toString());
    }

    private static void PutIntoMap(ServerNode node, File file2) {
        String fileName=file2.getName();
        if(fileName.contains("blk_")&&!fileName.endsWith(".meta"))
        {
            node.getBlockMap().put(fileName.split("_")[1],file2.getAbsolutePath());
        }
    }


    public  static ArrayList<ServerNode> ParseConfiuration(String filePath) throws IOException {
        ArrayList<ServerNode> serverNodeList=new ArrayList<ServerNode>();
        FileInputStream fis = new FileInputStream(filePath);
        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        while ((line = br.readLine()) != null) {
            String[]  m=line.split(" ");
            ServerNode node=new ServerNode(m[0],m[1],m[2],m[3]);
            serverNodeList.add(node);
        }
        br.close();
        System.out.println(serverNodeList.toString());
        return serverNodeList;
    }

}
