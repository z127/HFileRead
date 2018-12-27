package com.zq.hbase.hfile.hbasequery;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import static java.lang.String.valueOf;

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
        //get the hbase table instance



        HTable table = new HTable(conf,tablename);
        try {
            con = ConnectionFactory.createConnection(conf);
            List<HRegionInfo>   list=MetaScanner.listAllRegions(conf,con,false);
            System.out.println("list"+ list);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(tablename);
            Table querytable = con.getTable(hbasetablename);
            querytable.close();
            System.out.println("tableName : "+tablename);
            System.out.println("Data : ");

            Scan scan = new Scan();
            ResultScanner rsscan = table.getScanner(scan);
            for(Result rs : rsscan){
                System.out.println(Bytes.toString(rs.getRow()));
                for(Cell cell : rs.rawCells()){
                    System.out.println(
                            " family: "+Bytes.toString( CellUtil.cloneFamily(cell))
                                    +" -> "+
                                " column : "  +Bytes.toString(CellUtil.cloneQualifier(cell))
                                    +" -> "+
                                 " value : "  + Bytes.toString(CellUtil.cloneValue(cell))
                                    +" -> "+
                                  " timestamp : " + cell.getTimestamp()
                    );
                  String regioninfo=  Bytes.toString( CellUtil.cloneFamily(cell))+":"+Bytes.toString(CellUtil.cloneQualifier(cell));
                    if("info:regioninfo".equals(regioninfo)) {
                        HRegionInfo hRegionInfo = HRegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        System.out.println("startKey " + Bytes.toString(hRegionInfo.getStartKey()) + " endKey " + Bytes.toString(hRegionInfo.getEndKey())+"RegionInfo "+Bytes.toString(hRegionInfo.getRegionName()));
                    }
                    }

                System.out.println("------------------------------");
            }
            rsscan.close();
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


    public static void bigInsertTestImport(String tablename) throws IOException {
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
            for(int i=352;i<400;i++) {
                String rowKey = new DecimalFormat(Decimalformat).format(i);
                System.out.println("format: " + rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));
                //关闭写前日志
                //put.setWriteToWAL(false);
                put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name2"), Bytes.toBytes(valueOf(i+"tom")));
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
            for(int i=500000;i<502000;i++) {
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
                put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(valueOf(i+"tom")));
                put.add(Bytes.toBytes("cf2"), Bytes.toBytes("chinese"), Bytes.toBytes(valueOf(i+"chinese")));
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
