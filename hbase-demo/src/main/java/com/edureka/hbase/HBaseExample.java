package com.edureka.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.edureka.hbase.constants.HbaseConstants;
import com.edureka.hbase.repositories.SegmentRepository;
import com.edureka.hbase.repositories.TransactionRepository;
import com.edureka.hbase.utility.FileUtility;

public class HBaseExample {

	public static void main(String[] args) throws Exception {
		new HBaseExample().connect();
	}

	private void connect() throws Exception {
		Configuration config = HBaseConfiguration.create();

		// String path =
		// this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
		// config.addResource(new Path(path));

		Connection connection = ConnectionFactory.createConnection(config);

		Admin admin = connection.getAdmin();

		createTable(HbaseConstants.TRANSACTION, admin);
		createTable(HbaseConstants.SEGMENT, admin);

		String filePath = "/Users/raghugupta/Documents/Capstone/transaction.csv";

		TransactionRepository transactionRepository = new TransactionRepository(
				connection.getTable(TableName.valueOf(HbaseConstants.TRANSACTION)));

		SegmentRepository segmentRepository = new SegmentRepository(
				connection.getTable(TableName.valueOf(HbaseConstants.SEGMENT)));

		FileUtility.readFile(filePath, transactionRepository, segmentRepository);

		transactionRepository.scan(2l);

		transactionRepository.get("1006");

		segmentRepository.scan(2l);

		segmentRepository.scanByPrefix("Home");

	}

	private static void createTable(String tableName, Admin admin) {
		try {
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
				desc.addFamily(new HColumnDescriptor("info"));

				admin.createTable(desc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void deleteTable(String tableName, Admin admin) {
		try {
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
