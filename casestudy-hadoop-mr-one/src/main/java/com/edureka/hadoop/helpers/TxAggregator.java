package com.edureka.hadoop.helpers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TxAggregator extends Reducer<Text, AggregateWritable, Text, AggregateWritable> {

	public void reduce(Text key, Iterable<AggregateWritable> values, Context context)
			throws IOException, InterruptedException {

		AggregateData aggregateData = new AggregateData();
		AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);

		for (AggregateWritable val : values) {
			aggregateData
					.setOrderabove2000(aggregateData.getOrderabove2000() + val.getAggregateData().getOrderabove2000());
			aggregateData
					.setOrderbelow1000(aggregateData.getOrderbelow1000() + val.getAggregateData().getOrderbelow1000());
			aggregateData
					.setOrderbelow2000(aggregateData.getOrderbelow2000() + val.getAggregateData().getOrderbelow2000());
			aggregateData
					.setOrderbelow500(aggregateData.getOrderbelow500() + val.getAggregateData().getOrderbelow500());
			aggregateData.setTotalOrder(val.getAggregateData().getTotalOrder() + aggregateData.getTotalOrder());

		}
		context.write(key, aggregateWritable);
	}
}