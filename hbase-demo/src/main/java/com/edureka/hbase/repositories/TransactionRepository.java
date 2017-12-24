package com.edureka.hbase.repositories;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.hbase.pojo.MerchantTransaction;

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

	/**
	 * Getting merchants having order count greater then given order
	 * @param orderCount
	 * @return
	 */
	public MerchantTransaction scan(Long orderCount) {
		MerchantTransaction transaction = new MerchantTransaction();
		try {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("info"));
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("ordercount"), CompareOp.GREATER, Bytes.toBytes(orderCount+""));
			filter1.setLatestVersionOnly(true);
			scan.setFilter(filter1);
			ResultScanner scanner1 = txTable.getScanner(scan);
			for (Result result : scanner1) {
				byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("merchantId"));
				if (bytes != null)
					transaction.setMerchantId(Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ordercount"));
				if (bytes != null) {
					String value = Bytes.toString(bytes);
					if (value.trim().length() != 0) {
						transaction.setOrderCount(Long.parseLong(Bytes.toString(bytes)));
					}
				}
				logger.info("tx fetched = {} ", transaction);
			}
			scanner1.close();
		} catch (Exception e) {
			logger.error("exception occured = {} ", e);
		}
		return transaction;
	}
}
