package com.zq.hbase.hfile.hbasequery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class HFileDemo {

    public static String  NAME_SEPARATOR=",";
    public static String  CONTENT_SEPARATOR=",";
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {


	}

        public  static void     originalParseData() throws IOException {
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);
            //指向你的hdfs下，hfile的正确目录，我是cp到了新的目录
            //Path path = new Path("/test/0a99d83b2b0a49c0adbc371d4bfe021e");
            Path path = new Path("D:\\HbaseData\\Hbasedata\\hbase_student_cf1_00000");
            //Path path = new Path("H:\\HdumpData\\1abc");
            HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(config), config);
            HFileReaderV2  readerV2 = (HFileReaderV2) reader;
            HFileScanner  scan = readerV2.getScanner(false, false);
            long length = 0;
            long count = 0;
            scan.seekTo(reader.getFirstKey());
            do{
                KeyValue kv = (KeyValue) scan.getKeyValue();
                if(kv ==null)
                    continue;
                else {
                    System.out.println(" count "+count+" Family :"+new String(kv.getFamily())+" Column : "+new String(kv.getQualifier())+" value : "+new String(kv.getValue())+" rowkey : "+new String(kv.getRow())+" timestamp : "+kv.getTimestamp()+" version"+kv.getMvccVersion());

                }
                count ++;
                if(count==100)
                {
                    break;
                }
            }while(scan.next());

        }



	public static void ReadFile (String filePath)  {
	    try {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(filePath);
        HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(config), config);
        HFileReaderV2  readerV2 = (HFileReaderV2) reader;
        HFileScanner  scan = readerV2.getScanner(false, false);
        BufferedOutputStream Buff = null;
        long length = 0;
        long count = 0;
        scan.seekTo(reader.getFirstKey());
        Map<String,BufferedOutputStream> bufferedOutputStreamMap=new HashMap<String,BufferedOutputStream>();
        //获取文件
        //bufferedOutputStreamMap=generateMap("hbase_student2", scan);
        do{
            KeyValue kv = (KeyValue) scan.getKeyValue();
            if(kv ==null)
                continue;
            else {
                //生成文件

                    String key= Bytes.toString(kv.getQualifier());
                   if(bufferedOutputStreamMap.containsKey(key))
                   {
                       Buff=bufferedOutputStreamMap.get(key);
                   }else {
                       Buff=generateBufferedOutputStream("hbase_student2",kv);
                       bufferedOutputStreamMap.put(key,Buff);
                   }
                  // String content=new String(kv.getRow())+HFileDemo.CONTENT_SEPARATOR+new String(kv.getFamily())+HFileDemo.CONTENT_SEPARATOR+new String(kv.getQualifier())+HFileDemo.CONTENT_SEPARATOR+new String(kv.getValue())+"\r\n";
                    String content=new String(kv.getRow())+HFileDemo.CONTENT_SEPARATOR+new String(kv.getValue())+"\r\n";
                    Buff.write(content.getBytes(),0,content.getBytes().length);

            }

            count ++;
        }while(scan.next());
        System.out.println("写完了");
        Iterator<Map.Entry<String, BufferedOutputStream>> entries = bufferedOutputStreamMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, BufferedOutputStream> entry = entries.next();
           // System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            entry.getValue().flush();
            entry.getValue().close();
        }
        }catch (IOException m){
            System.out.println("IOEXCEPTION");
        }
    }

    /**
     *
     * @param TableName
     * @param scan
     * @return
     * @throws IOException
     */
    private static Map<String,BufferedOutputStream> generateMap(String TableName, HFileScanner  scan) throws IOException {
	  HashMap  fileMap=new HashMap<String,File>();
        KeyValue kv;
        do{
           kv= (KeyValue) scan.getKeyValue();
            if(kv ==null)
                continue;
            else {
                if(!fileMap.containsKey(new String(kv.getQualifier())))
                {
                    fileMap.put(new String(kv.getQualifier()),generateBufferedOutputStream(TableName, kv));
                }
            }
        }while(scan.next());
        return fileMap;
    }

    /**
     * 获取写入文件的流
     * @param TableName
     * @param kv
     * @return
     * @throws IOException
     */
    private static BufferedOutputStream generateBufferedOutputStream(String TableName, KeyValue kv) throws IOException {
        FileOutputStream outSTr = null;
        BufferedOutputStream Buff = null;
	    String FileName=TableName+ HFileDemo.NAME_SEPARATOR+new String(kv.getFamily())+HFileDemo.NAME_SEPARATOR+new String(kv.getQualifier());
        String FilePath=System.getProperty("user.dir")+"\\"+FileName;
        String  charSet="utf-8";
        File file=new File(FileName);
        //文件不存在
        if(!fileExists(FileName))
        {
        file.createNewFile();
        }
        outSTr = new FileOutputStream(file,true);
        Buff = new BufferedOutputStream(outSTr);
        return Buff;
    }


    public static  boolean fileExists(String plainFilePath){
        File file=new File(plainFilePath);
        if(file.exists()) {
          return  true;
        } else{
            return false;
        }
    }


    public static void generateFile (String filePath,String generateFile) throws IOException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(filePath);
        HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(config), config);
        HFileReaderV2  readerV2 = (HFileReaderV2) reader;
        HFileScanner  scan = readerV2.getScanner(false, false);

        long length = 0;
        long count = 0;
        scan.seekTo(reader.getFirstKey());
        do{
            KeyValue kv = (KeyValue) scan.getKeyValue();
            if(kv ==null)
                continue;
            else {
                System.out.println(" count "+count+" Family :"+new String(kv.getFamily())+" Column : "+new String(kv.getQualifier())+" value : "+new String(kv.getValue())+" rowkey : "+new String(kv.getRow())+" timestamp : "+kv.getTimestamp()+" version"+kv.getMvccVersion());
            }
            count ++;
            if(count==100)
            {
                break;
            }
        }while(scan.next());




    }










}
