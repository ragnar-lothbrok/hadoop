package com.edureka.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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
		
//		admin.disableTable(TableName.valueOf("transaction"));
//		
//		admin.deleteTable(TableName.valueOf("transaction"));

		if (!admin.tableExists(TableName.valueOf("transaction"))) {
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("transaction"));
			desc.addFamily(new HColumnDescriptor("info"));

			admin.createTable(desc);
		}

		String filePath = "/Users/raghugupta/Documents/Capstone/transaction.csv";

		TransactionRepository transactionRepository = new TransactionRepository(
				connection.getTable(TableName.valueOf("transaction")));

		FileUtility.readFile(filePath, transactionRepository);

	}
}
