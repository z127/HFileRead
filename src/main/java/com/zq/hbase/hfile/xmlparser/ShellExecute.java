package com.zq.hbase.hfile.xmlparser;

import com.zq.hbase.hfile.hbasequery.HFileDemo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public class ShellExecute {
    public static final String WORD_SEPERATOR=" ";
    public static void main(String[] args) throws IOException, InterruptedException {
        //执行shell脚本获取block文件
        List<String> params=new ArrayList<String>();
        params.add(args[1]);
        params.add(args[2]);
        params.add(args[3]);
        params.add(args[4]);
        params.add(args[5]);
        ExecCMD(args[0],params);
    }

    public  static  void ExecCMD(String filePath,List<String> paramList) throws IOException, InterruptedException {
        Process process = null;
        String cmd=filePath;
        List<String> processList = new ArrayList<String>();
        int runningStatus=0;
        for(int i=0;i<paramList.size();i++)
        {
            cmd+=WORD_SEPERATOR+paramList.get(i);
        }
        System.out.println(cmd);
        process=Runtime.getRuntime().exec(cmd);
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        while ((line = input.readLine()) != null) {
            processList.add(line);
        }
        runningStatus=process.waitFor();
        input.close();
        if(runningStatus!=0)
        {
            System.out.println("shell script errors");
        }
        for(String lineContent :processList)
        {
            System.out.println(lineContent);
        }
        System.out.println("内容结束了,生成txt文件");
        String[] splitList=paramList.get(3).split("/");
        HFileDemo.ReadFile(paramList.get(4)+"/"+splitList[splitList.length-1]);
    }
}
