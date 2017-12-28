package com.edureka.hadoop.helpers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionMapper extends Mapper<Object, Text, Text, AggregateWritable> {

	// Map method
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().replace("\"", "");

		if (line.indexOf("transaction") != -1) {
			return;
		}
		String split[] = line.split(",");

		Transaction transaction = new Transaction();
		transaction.setTxId(split[0]);
		transaction.setCustomerId(Long.parseLong(split[1]));
		transaction.setMerchantId(Long.parseLong(split[2]));
		transaction.setTimestamp(split[3].split(" ")[0].trim());
		transaction.setInvoiceNum(split[4].trim());
		transaction.setInvoiceAmount(Float.parseFloat(split[5]));
		transaction.setSegment(split[6].trim());

		AggregateData aggregateData = new AggregateData();

		AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);

		if (transaction.getInvoiceAmount() <= 500) {
			aggregateData.setOrderbelow500(1l);
		} else if (transaction.getInvoiceAmount() > 500 && transaction.getInvoiceAmount() <= 1000) {
			aggregateData.setOrderbelow1000(1l);
		} else if (transaction.getInvoiceAmount() > 1000 && transaction.getInvoiceAmount() < 2000) {
			aggregateData.setOrderbelow2000(1l);
		} else {
			aggregateData.setOrderabove2000(1l);
		}

		aggregateData.setTotalOrder(1l);

		context.write(new Text(transaction.getTimestamp() + "-" + transaction.getSegment()), aggregateWritable);
	}

}
