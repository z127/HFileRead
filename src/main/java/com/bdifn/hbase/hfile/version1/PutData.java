package com.bdifn.hbase.hfile.version1;

import java.io.IOException;

public class PutData {


    public static void main(String[] args) throws IOException {
        HBaseUtils.getInstance().bigInsert("hbase_student");
        HBaseUtils.getInstance().get("hbase_student");
    }
}
