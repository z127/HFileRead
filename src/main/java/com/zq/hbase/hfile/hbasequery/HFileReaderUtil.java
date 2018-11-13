package com.zq.hbase.hfile.hbasequery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HFileReaderUtil {

    private Configuration conf ;

    private Path path ;

    private HFile.Reader reader;

    private HFileScanner scanner;

    public HFileReaderUtil()  {
        if(conf==null){
            conf= HBaseConfiguration.create();
        }

    }

    public void scanHfile(String pathstr)throws IOException {
        path = new Path(pathstr);
        reader = HFile.createReader(FileSystem.get(conf),path ,new CacheConfig(conf),conf);
        scanner = reader.getScanner(false,false);
        reader.loadFileInfo();
        scanner.seekTo();

        do{
            KeyValue kv = (KeyValue) scanner.getKeyValue();
            System.out.println("rowkey = "+ Bytes.toString(CellUtil.cloneRow(kv)));
            System.out.println("cf = "+Bytes.toString(CellUtil.cloneFamily(kv)));
            System.out.println("column value = "+Bytes.toString(CellUtil.cloneValue(kv)));
            System.out.println("column name = "+CellUtil.cloneQualifier(kv));

        }while (scanner.next());

    }



}