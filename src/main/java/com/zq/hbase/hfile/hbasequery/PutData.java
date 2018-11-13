package com.zq.hbase.hfile.hbasequery;

import java.io.IOException;

public class PutData {


    public static void main(String[] args) throws IOException {
        //HBaseUtils.getInstance().bigInsert("hbase_student");
       //HBaseUtils.getInstance().get("hbase_student");
        String path="D:\\HbaseData\\Hbasedata\\hbase_student_cf1_390004";
        HFileDemo.ReadFile(path);
        System.out.println(System.getProperty("user.dir"));
    }
}
