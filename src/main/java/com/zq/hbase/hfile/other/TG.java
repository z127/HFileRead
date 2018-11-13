package com.zq.hbase.hfile.other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TG {

	public static String columnFamily = "info";
	private static HConnection conn;

	public static final byte[] INFO_FC = Bytes.toBytes("info");

	public static final byte[] COLUMN_ID = Bytes.toBytes("id");
	public static final byte[] COLUMN_NAME = Bytes.toBytes("name");
	public static final byte[] COLUMN_EMAIL = Bytes.toBytes("email");
	public static final byte[] COLUMN_AGE = Bytes.toBytes("age");
	public static final byte[] COLUMN_SEX = Bytes.toBytes("sex");
	public static final byte[] COLUMN_PASSWORD = Bytes.toBytes("password");
	public static final byte[] COLUMN_HEIGHT = Bytes.toBytes("height");

	Configuration config = null;

	public TG() {
		config = HBaseConfiguration.create();
	}

	public void launch() throws Exception {
		String tableName = "t_sample";
		conn = HConnectionManager.createConnection(HBaseConfiguration.create());
		try {
			// step 1. create table
			createTable(tableName, columnFamily, 1);

			// step 2. provisioning data.
			int batchSize = 10;
			int batchCount = 10000;

			for (int i = 0; i < batchCount; i++) {
				initDatas(tableName, batchSize, i * batchSize);
			}
		} finally {
			conn.close();
		}

	}

	public static void initDatas(String tableName, int dataPerTable, int base)
			throws Exception {
		long start = System.currentTimeMillis();
		List<Put> puts = new ArrayList<Put>();
		for (int i = base; i < dataPerTable + base; i++) {

			Put put = new Put(Bytes.toBytes(i));
			put.add(INFO_FC, COLUMN_ID, Bytes.toBytes("u" + i));
			put.add(INFO_FC, COLUMN_NAME, Bytes.toBytes("u" + i));
			put.add(INFO_FC, COLUMN_EMAIL, Bytes.toBytes("u" + i + "@test.com"));
			put.add(INFO_FC, COLUMN_AGE, Bytes.toBytes(i % 30));
			put.add(INFO_FC, COLUMN_SEX,
					Bytes.toBytes((i % 2 == 0) ? "M" : "F"));
			put.add(INFO_FC, COLUMN_PASSWORD, Bytes.toBytes("p" + i));
			put.add(INFO_FC, COLUMN_HEIGHT, Bytes.toBytes(160 + i % 20));

			puts.add(put);

		}

		HTableInterface htable = null;
		try {
			htable = conn.getTable(tableName);
			htable.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Insert finish rows : " + puts.size()
				+ "total times : " + (System.currentTimeMillis() - start)
				+ "ms, sleep 5ms.");
		puts.clear();
		puts = null;
		try {
			Thread.sleep(5);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createTable(String tableName, String column, int version)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
			} catch (Exception e) {
				e.printStackTrace();
			}
			admin.deleteTable(tableName);
		}

		TableName tbName = TableName.valueOf(tableName);
		HTableDescriptor tableDesc = new HTableDescriptor(tbName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(column);
		columnDesc.setMaxVersions(version);
		tableDesc.addFamily(columnDesc);

		admin.createTable(tableDesc);

		admin.close();
	}

}
