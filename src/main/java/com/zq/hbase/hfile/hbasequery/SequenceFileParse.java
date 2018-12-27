package com.zq.hbase.hfile.hbasequery;

import com.google.gson.Gson;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.zq.hbase.hfile.entity.ActionNode;
import com.zq.hbase.hfile.entity.HFileSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HFileProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class SequenceFileParse {

    private boolean outputValues;
    private boolean outputJSON;
    private long sequence;
    private String region;
    private String row;
    private boolean persistentOutput;
    private boolean firstTxn;
    private PrintStream out;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public  SequenceFileParse() {
        this.outputValues = true;
        this.outputJSON = true;
        this.sequence = -1L;
        this.region = null;
        this.row = null;
        this.persistentOutput = false;
        this.firstTxn = true;
        this.out = System.out;
    }

        public static final String output_Metapath = "D:\\meta.meta";
        public static final String output_Region = "D:\\regioninfo";
        private static final String[] DATA = { "a", "b", "c", "d"};



        public static void main(String[] args) throws IOException {
           // write(output_path);
           // ArrayList<HFileSequence> list=parseHlogFile(output_path);
            //ArrayList<HFileSequence> list=parseMetaHlogFile(output_Metapath);
            //putDataToHBase(list,"default","testimport2");
            parseRegionFile(output_Region);
        }



    private static  void parseRegionFile( String filePath) {
        Path path = new Path(filePath);
        File file = new File(filePath);
        BufferedReader reader = null;
        try{
            String tempString = null;
            int tempchar;
            reader = new BufferedReader(new FileReader(file));
            while ((tempchar = reader.read()) != -1) {
                // 对于windows下，\r\n这两个字符在一起时，表示一个换行。
                // 但如果这两个字符分开显示时，会换两次行。
                // 因此，屏蔽掉\r，或者屏蔽\n。否则，将会多出很多空行。
                if (((char) tempchar) != '\r') {
                    System.out.print((char) tempchar);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(reader!=null)
            {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //hRegionInfo = HRegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        //String   val=  "startKey:" + Bytes.toString(hRegionInfo.getStartKey()) + ",endKey:" + Bytes.toString(hRegionInfo.getEndKey())+",RegionInfo:"+hRegionInfo.getEncodedName();
        //   System.out.println(val);
        //op.put("vlen", val);

    }

    private static void putDataToHBase(ArrayList<HFileSequence> list, String namespace,String testimport2) throws IOException {
            String tableName=namespace+":"+testimport2;
            for(int i=0;i<list.size();i++)
            {
                HFileSequence sequenceItem=list.get(i);
                if(tableName.equals(sequenceItem.getTable().getNameWithNamespaceInclAsString()))
                {
                       List<ActionNode> actionList=sequenceItem.getActions();
                        InsertData(actionList,namespace,testimport2);
                }
            }

    }

    private static void InsertData(List<ActionNode> actionList, String namespace, String tablename) throws IOException {
        long start=System.currentTimeMillis();
        Configuration conf= HBaseConfiguration.create();
        Connection con=null;
        try {
            con = ConnectionFactory.createConnection(conf);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(namespace+":"+tablename);
            HTable table = (HTable) con.getTable(hbasetablename);

            //不要自动清理缓冲区
            table.setAutoFlush(false);
            for(int i=0;i<actionList.size();i++) {
               ActionNode node=actionList.get(i);
                String rowKey =node.getRow();
                System.out.println("format: " + rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));
                //关闭写前日志
                //put.setWriteToWAL(false);
                put.add(Bytes.toBytes(node.getFamily()), Bytes.toBytes(node.getQualifier()), Bytes.toBytes(node.getVlen()));
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


    public  static   ArrayList<HFileSequence> parseHlogFile(String File) throws IOException {
            Configuration config = new Configuration();
            //指向你的hdfs下，hfile的正确目录，我是cp到了新的目录
            //Path path = new Path("/test/0a99d83b2b0a49c0adbc371d4bfe021e");
            Path path = new Path(File);
            //Path path = new Path("H:\\HdumpData\\1abc");
              return    new SequenceFileParse().processFile(config,path);
        }

    public  static   ArrayList<HFileSequence> parseMetaHlogFile(String File) throws IOException {
        Configuration config = new Configuration();
        //指向你的hdfs下，hfile的正确目录，我是cp到了新的目录
        //Path path = new Path("/test/0a99d83b2b0a49c0adbc371d4bfe021e");
        Path path = new Path(File);
        //Path path = new Path("H:\\HdumpData\\1abc");
        return    new SequenceFileParse().processMetaFile(config,path);
    }




    public  ArrayList<HFileSequence> processFile(Configuration conf, Path p) throws IOException {
        Gson gson = new Gson();
        FileSystem fs = p.getFileSystem(conf);
        if (!fs.exists(p)) {
            throw new FileNotFoundException(p.toString());
        } else if (!fs.isFile(p)) {
            throw new IOException(p + " is not a file");
        } else {
            WAL.Reader log = WALFactory.createReader(fs, p, conf);
            if (log instanceof ProtobufLogReader) {
                List<String> writerClsNames = ((ProtobufLogReader)log).getWriterClsNames();
                if (writerClsNames != null && writerClsNames.size() > 0) {
                    this.out.print("Writer Classes: ");
                    for(int i = 0; i < writerClsNames.size(); ++i) {
                        this.out.print((String)writerClsNames.get(i));
                        if (i != writerClsNames.size() - 1) {
                            this.out.print(" ");
                        }
                    }

                    this.out.println();
                }

                String cellCodecClsName = ((ProtobufLogReader)log).getCodecClsName();
                if (cellCodecClsName != null) {
                    this.out.println("Cell Codec Class: " + cellCodecClsName);
                }
            }

            if (this.outputJSON && !this.persistentOutput) {
                this.out.print("[");
                this.firstTxn = true;
            }
            ArrayList<HFileSequence> sequencesList = new ArrayList<HFileSequence>();
            WAL.Entry entry;
            try {
                while((entry = log.next()) != null) {
                    WALKey key = entry.getKey();
                    WALEdit edit = entry.getEdit();
                    Map<String, Object> txn = key.toStringMap();
                    long writeTime = key.getWriteTime();
                    if ((this.sequence < 0L || ((Long)txn.get("sequence")).longValue() == this.sequence) && (this.region == null || ((String)txn.get("region")).equals(this.region))) {
                        List<Map> actions = new ArrayList();
                        Iterator i$ = edit.getCells().iterator();

                        while(i$.hasNext()) {
                            Cell cell = (Cell)i$.next();
                            Map<String, Object> op = new HashMap(toStringMap(cell));
                            if (this.outputValues) {
                                op.put("value", Bytes.toStringBinary(cell.getValue()));
                               /* String regioninfo=  Bytes.toString( CellUtil.cloneFamily(cell))+":"+Bytes.toString(CellUtil.cloneQualifier(cell));
                                HRegionInfo hRegionInfo;
                                if("info:regioninfo".equals(regioninfo)) {
                                     hRegionInfo = HRegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                    System.out.println("startKey " + Bytes.toString(hRegionInfo.getStartKey()) + " endKey " + Bytes.toString(hRegionInfo.getEndKey())+" RegionInfo "+hRegionInfo.getEncodedName());
                                }*/
                            }
                            if (this.row == null || ((String)op.get("row")).equals(this.row)) {
                                actions.add(op);
                            }
                        }

                        if (actions.size() != 0) {
                            txn.put("actions", actions);
                            if (this.outputJSON) {
                                if (this.firstTxn) {
                                    this.firstTxn = false;
                                } else {
                                    this.out.print(",");
                                }

                                //this.out.print(MAPPER.writeValueAsString(txn));
                                HFileSequence hFileSequence=gson.fromJson(MAPPER.writeValueAsString(txn),HFileSequence.class);
                              // System.out.println("hfile"+hFileSequence.toString());
                                sequencesList.add(hFileSequence);
                                System.out.println(sequencesList.size());
                               // System.out.println("zhouzhou"+MAPPER.writeValueAsString(txn));
                            } else {
                                this.out.println("Sequence=" + txn.get("sequence") + " " + ", region=" + txn.get("region") + " at write timestamp=" + new Date(writeTime));
                                for(int i = 0; i < actions.size(); ++i) {
                                    Map op = (Map)actions.get(i);
                                    this.out.println("row=" + op.get("row") + ", column=" + op.get("family") + ":" + op.get("qualifier"));
                                    if (op.get("tag") != null) {
                                        this.out.println("    tag: " + op.get("tag"));
                                    }
                                    if (this.outputValues) {
                                        this.out.println("    value: " + op.get("value"));
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                log.close();
            }

            for (HFileSequence node:sequencesList) {
                System.out.println(node.toString());
            }

            if (this.outputJSON && !this.persistentOutput) {
                this.out.print("]");
            }
            return sequencesList;
        }
    }

    public  ArrayList<HFileSequence> processMetaFile(Configuration conf, Path p) throws IOException {
        Gson gson = new Gson();
        FileSystem fs = p.getFileSystem(conf);
        if (!fs.exists(p)) {
            throw new FileNotFoundException(p.toString());
        } else if (!fs.isFile(p)) {
            throw new IOException(p + " is not a file");
        } else {
            WAL.Reader log = WALFactory.createReader(fs, p, conf);
            if (this.outputJSON && !this.persistentOutput) {
                this.out.print("[");
                this.firstTxn = true;
            }
            ArrayList<HFileSequence> sequencesList = new ArrayList<HFileSequence>();
            WAL.Entry entry;
            try {
                while((entry = log.next()) != null) {
                    WALKey key = entry.getKey();
                    WALEdit edit = entry.getEdit();
                    Map<String, Object> txn = key.toStringMap();
                    long writeTime = key.getWriteTime();
                    if ((this.sequence < 0L || ((Long)txn.get("sequence")).longValue() == this.sequence) && (this.region == null || ((String)txn.get("region")).equals(this.region))) {
                        List<Map> actions = new ArrayList();
                        Iterator i$ = edit.getCells().iterator();

                        while(i$.hasNext()) {
                                Cell cell = (Cell)i$.next();
                                 Map<String, Object> op = new HashMap(toStringMap(cell));
                                op.put("vlen", Bytes.toStringBinary(cell.getValue()));
                                String regioninfo=  Bytes.toString( CellUtil.cloneFamily(cell))+":"+Bytes.toString(CellUtil.cloneQualifier(cell));
                                HRegionInfo hRegionInfo;
                                if("info:regioninfo".equals(regioninfo)) {
                                     hRegionInfo = HRegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                    String   val=  "startKey:" + Bytes.toString(hRegionInfo.getStartKey()) + ",endKey:" + Bytes.toString(hRegionInfo.getEndKey())+",RegionInfo:"+hRegionInfo.getEncodedName();
                                 //   System.out.println(val);
                                    op.put("vlen", val);
                                }
                            if (this.row == null || ((String)op.get("row")).equals(this.row)) {
                                actions.add(op);
                            }
                        }

                        if (actions.size() != 0) {
                            txn.put("actions", actions);
                            if (this.outputJSON) {
                                if (this.firstTxn) {
                                    this.firstTxn = false;
                                } else {
                                    this.out.print(",");
                                }
                                //this.out.print(MAPPER.writeValueAsString(txn));
                                HFileSequence hFileSequence=gson.fromJson(MAPPER.writeValueAsString(txn),HFileSequence.class);
                                System.out.println(hFileSequence.toString());
                                sequencesList.add(hFileSequence);
                                 //System.out.println(sequencesList.size());
                                // System.out.println("zhouzhou"+MAPPER.writeValueAsString(txn));
                            }
                        }
                    }
                }
            } finally {
                log.close();
            }

         /*   for (HFileSequence node:sequencesList) {
                System.out.println(node.toString());
            }*/

            if (this.outputJSON && !this.persistentOutput) {
                this.out.print("]");
            }
            return sequencesList;
        }
    }

    private static Map<String, Object> toStringMap(Cell cell) {
        Map<String, Object> stringMap = new HashMap();
        stringMap.put("row", Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        stringMap.put("family", Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
        stringMap.put("qualifier", Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
        stringMap.put("timestamp", cell.getTimestamp());
        stringMap.put("vlen", Bytes.toStringBinary(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        if (cell.getTagsLength() > 0) {
            List<String> tagsString = new ArrayList();
            Iterator tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());

            while(tagsIterator.hasNext()) {
                Tag tag = (Tag)tagsIterator.next();
                tagsString.add(tag.getType() + ":" + Bytes.toStringBinary(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength()));
            }

            stringMap.put("tag", tagsString);
        }

        return stringMap;
    }


    public void traverseFolder2(String TableName) {

        File file = new File(TableName);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder2(file2.getAbsolutePath());
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

}
