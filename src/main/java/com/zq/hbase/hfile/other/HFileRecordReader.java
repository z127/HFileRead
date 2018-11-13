package com.zq.hbase.hfile.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class HFileRecordReader<K,V>extends RecordReader{


    private HFile.Reader reader;
    private final HFileScanner scanner;
    private int entryNumber = 0;

    public HFileRecordReader(FileSplit split, Configuration conf)
            throws IOException {
        final Path path = split.getPath();
        reader = HFile.createReader(FileSystem.get(conf), path,new CacheConfig(conf), conf);
        scanner = reader.getScanner(false, false);
        reader.loadFileInfo();
        scanner.seekTo();
    }



    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        entryNumber++;
        return scanner.next();
    }

    public Object getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return new ImmutableBytesWritable(scanner.getKeyValue().getRow());
    }

    public Object getCurrentValue() throws IOException, InterruptedException {
        return scanner.getKeyValue();
    }


    /**
     * 返回运行进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        if (reader != null) {
            return (entryNumber / reader.getEntries());
        }
        return 1;
    }


    /**
     * 关闭读取资源
     * @throws IOException
     */
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}