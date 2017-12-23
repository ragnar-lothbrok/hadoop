package com.edureka.hadoop.custominput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

public class CustomWritable implements Writable {

	private static Gson gson = new Gson();

	private OrderData aggregateData = new OrderData();

	public CustomWritable() {

	}

	public CustomWritable(OrderData aggregateData) {
		super();
		this.aggregateData = aggregateData;
	}

	public OrderData getAggregateData() {
		return aggregateData;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		aggregateData.setTotalOrder(in.readDouble());
		aggregateData.setTotalOrderValue(in.readDouble());
		aggregateData.setTotalDiscountValue(in.readDouble());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(aggregateData.getTotalOrder());
		out.writeDouble(aggregateData.getTotalOrderValue());
		out.writeDouble(aggregateData.getTotalDiscountValue());
	}

	@Override
	public String toString() {
		return gson.toJson(aggregateData);
	}

}
