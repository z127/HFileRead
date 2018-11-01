package com.bdifn.hbase.hfile.version1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

public class HBaseUtils {
    HBaseAdmin admin=null;
    Configuration configuration =null;
    private HBaseUtils(){
        configuration=new Configuration();
        configuration.set("hbase.zookeeper.quorum","s101:2181,s102:2181,s103:2181");
        configuration.set("hbase.rootdir","hdfs://s101:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static volatile HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }



    /**
     * 根据表名获取到 Htable 实例
     */
    public HTable getHTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
     * @param tableName Hbase 表名
     * @param rowkey Hbase 表的 rowkey * @param cf Hbase 表的 columnfamily * @param column Hbase 表的列
     * @param value 写入 Hbase 表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table = getHTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }






    public static void querytable(String tablename) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con=null;
        try {
            con = ConnectionFactory.createConnection(conf);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(tablename);
            Table querytable = con.getTable(hbasetablename);
            ResultScanner rs = null;
            Scan scan = new Scan();
            rs = querytable.getScanner(scan);
            querytable.close();
            System.out.println("tableName : "+tablename);
            System.out.println("Data : ");
            for (Result r : rs)
                for (KeyValue kv : r.raw()) {
                    System.out.println(kv);
                    StringBuffer sb = new StringBuffer()
                            .append(Bytes.toString(kv.getRow())).append("\t")
                            .append(Bytes.toString(kv.getFamily()))
                            .append("\t")
                            .append(Bytes.toString(kv.getQualifier()))
                            .append("\t").append(Bytes.toLong(kv.getValue()));
                    System.out.println(sb.toString());
                }
            rs.close();
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally{
            con.close();
        }
    }


    public static void get(String tablename) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con=null;
        try {
            con = ConnectionFactory.createConnection(conf);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(tablename);
            Table querytable = con.getTable(hbasetablename);
            ResultScanner rs = null;
            Scan scan = new Scan();
            rs = querytable.getScanner(scan);
            querytable.close();
            System.out.println("tableName : "+tablename);
            System.out.println("Data : ");

            for (Result r : rs) {
                Cell[]    cellArr=r.rawCells();
                for(Cell cell:cellArr)
                {
                    //rowArray
                   byte[]  rowArr=cell.getRowArray();
                   //获取列族
                    String family = new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    System.out.println(cell.getTimestamp()+"  "+new String(cell.getValue(),"utf-8"));  //都转换为utf-8，中文会乱码
                    //获取rowkey
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //获取列名
                    String column= Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("row :"+row+" family :"+family+" column :"+column+" value : "+value);
                   // System.out.println(new String(rowArr));
                }
            }
            rs.close();
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally{
            con.close();
        }
    }


    public static void bigInsert(String tablename) throws IOException {
        long start=System.currentTimeMillis();
        Configuration conf= HBaseConfiguration.create();
        Connection con=null;
        String Decimalformat="0000000000";
        try {
            con = ConnectionFactory.createConnection(conf);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(tablename);
            HTable table = (HTable) con.getTable(hbasetablename);

            //不要自动清理缓冲区
            table.setAutoFlush(false);
            for(int i=0;i<1000000;i++) {
                String rowKey = new DecimalFormat(Decimalformat).format(i);
                System.out.println("format: " + rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));
                //关闭写前日志
                put.setWriteToWAL(false);
                if(i%2==0) {
                    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("gender"), Bytes.toBytes("man"));
                }else {
                    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("gender"), Bytes.toBytes("woman"));
                }
                put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(i+"tom")));
                put.add(Bytes.toBytes("cf2"), Bytes.toBytes("chinese"), Bytes.toBytes(String.valueOf(i%2000)));
                table.put(put);
                if(i % 2000==0)
                {
                    table.flushCommits();
                }
            }
            //自动清理
            table.flushCommits();
            System.out.println(System.currentTimeMillis()-start);
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally{
            con.close();
        }
    }


    private void ConFactoryWay() throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con    = ConnectionFactory.createConnection(conf);
        TableName[] names=con.getAdmin().listTableNames();
        System.out.println(" 123 "+names[0].getNameAsString());


    }


}
