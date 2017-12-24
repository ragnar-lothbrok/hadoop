package com.edureka.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionRepository {

	private static final Logger logger = LoggerFactory.getLogger(TransactionRepository.class);

	private Table txTable;

	public TransactionRepository(final Table txTable) {
		this.txTable = txTable;
	}

	public void put(MerchantTransaction transaction) {
		Put put = new Put(Bytes.toBytes(transaction.getMerchantId()));

		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("merchantId"), Bytes.toBytes(transaction.getMerchantId()));

		MerchantTransaction existingTx = get(transaction.getMerchantId());

		Long orderCount = transaction.getOrderCount();

		if (existingTx != null && existingTx.getMerchantId() != null) {
			orderCount += existingTx.getOrderCount();
		}
		
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ordercount"), Bytes.toBytes(orderCount + ""));

		try {
			txTable.put(put);
		} catch (IOException e) {
			logger.error("exception occured = {} ", e);
		}
		logger.info("tx added.");
	}

	public MerchantTransaction get(String merchantId) {
		MerchantTransaction transaction = new MerchantTransaction();
		try {

			Get get = new Get(Bytes.toBytes(merchantId));

			Result result = txTable.get(get);

			byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("merchantId"));
			if (bytes != null)
				transaction.setMerchantId(Bytes.toString(bytes));
			bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ordercount"));
			if (bytes != null)
				transaction.setOrderCount(Long.parseLong(Bytes.toString(bytes)));
			logger.info("tx fetched = {} ", transaction);
		} catch (Exception e) {
			logger.error("exception occured = {} ", e);
		}
		return transaction;
	}
}
