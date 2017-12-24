package com.edureka.hbase.repositories;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.hbase.pojo.SegmentOrder;

public class SegmentRepository {

	private static final Logger logger = LoggerFactory.getLogger(SegmentRepository.class);

	private Table txTable;

	public SegmentRepository(final Table txTable) {
		this.txTable = txTable;
	}

	public void put(SegmentOrder segment) {
		Put put = new Put(Bytes.toBytes(segment.getSegment()));

		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("segment"), Bytes.toBytes(segment.getSegment()));

		SegmentOrder existingSegment = get(segment.getSegment());

		Long orderCount = segment.getOrderCount();

		if (existingSegment != null && existingSegment.getSegment() != null) {
			orderCount += existingSegment.getOrderCount();
		}

		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ordercount"), Bytes.toBytes(orderCount + ""));

		try {
			txTable.put(put);
		} catch (IOException e) {
			logger.error("exception occured = {} ", e);
		}
		logger.info("tx added.");
	}

	public SegmentOrder get(String merchantId) {
		SegmentOrder segment = new SegmentOrder();
		try {

			Get get = new Get(Bytes.toBytes(merchantId));

			Result result = txTable.get(get);

			byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("segment"));
			if (bytes != null)
				segment.setSegment(Bytes.toString(bytes));
			bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ordercount"));
			if (bytes != null)
				segment.setOrderCount(Long.parseLong(Bytes.toString(bytes)));
			logger.info("tx fetched = {} ", segment);
		} catch (Exception e) {
			logger.error("exception occured = {} ", e);
		}
		return segment;
	}

	/**
	 * Getting merchants having order count greater then given order
	 * 
	 * @param orderCount
	 * @return
	 */
	public void scan(Long orderCount) {
		try {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("info"));
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("ordercount"), CompareOp.GREATER, Bytes.toBytes(orderCount + ""));
			filter1.setLatestVersionOnly(true);
			scan.setFilter(filter1);
			ResultScanner scanner1 = txTable.getScanner(scan);
			for (Result result : scanner1) {
				SegmentOrder segment = new SegmentOrder();
				byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("segment"));
				if (bytes != null)
					segment.setSegment(Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ordercount"));
				if (bytes != null) {
					String value = Bytes.toString(bytes);
					if (value.trim().length() != 0) {
						segment.setOrderCount(Long.parseLong(Bytes.toString(bytes)));
					}
				}
				logger.info("tx fetched = {} ", segment);
			}
			scanner1.close();
		} catch (Exception e) {
			logger.error("exception occured = {} ", e);
		}
	}

	public void scanByPrefix(String prefix) {
		try {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("info"));
			PrefixFilter filter1 = new PrefixFilter(Bytes.toBytes(prefix));
			scan.setFilter(filter1);
			ResultScanner scanner1 = txTable.getScanner(scan);
			for (Result result : scanner1) {
				SegmentOrder segment = new SegmentOrder();
				byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("segment"));
				if (bytes != null)
					segment.setSegment(Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ordercount"));
				if (bytes != null) {
					String value = Bytes.toString(bytes);
					if (value.trim().length() != 0) {
						segment.setOrderCount(Long.parseLong(Bytes.toString(bytes)));
					}
				}
				logger.info("tx fetched = {} ", segment);
			}
			scanner1.close();
		} catch (Exception e) {
			logger.error("exception occured = {} ", e);
		}
	}
}
