package com.bdifn.hbase.hfile.version1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class HFileDemo {
	public String getString(byte[] encodedText) throws UnsupportedEncodingException {
		final Base64.Decoder decoder = Base64.getDecoder();
		String m=new String(decoder.decode(encodedText));
		return  m;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();

		FileSystem fs = FileSystem.get(config);

		//指向你的hdfs下，hfile的正确目录，我是cp到了新的目录
		//Path path = new Path("/test/0a99d83b2b0a49c0adbc371d4bfe021e");
		Path path = new Path("H:\\HdumpData\\stockmu\\30304ab20ebf44e8b76bf0b5610bc15f");
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
				System.out.println("count"+count+"Family :"+new String(kv.getFamily())+" Column : "+new String(kv.getQualifier())+" value : "+new String(kv.getValue())+" rowkey : "+new String(kv.getRow())+"timestamp : "+kv.getTimestamp()+" version"+kv.getMvccVersion());
			}
			// EntrySize + kv.realSize
			//length += ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + kv.heapSize());
			count ++;
			if(count==100)
			{
				break;
			}
		}while(scan.next());

		//System.out.println(String.format("MemStoreSize:%d,HFileSize:%d,KeyValue Count:%d", length,reader.length(),count));
	}



}
