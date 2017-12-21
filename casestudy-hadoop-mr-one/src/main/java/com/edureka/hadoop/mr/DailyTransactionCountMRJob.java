package com.edureka.hadoop.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edureka.hadoop.helpers.AggregateData;
import com.edureka.hadoop.helpers.AggregateWritable;
import com.edureka.hadoop.helpers.CustomPartitioner;
import com.edureka.hadoop.helpers.Transaction;

public class DailyTransactionCountMRJob {

	public static class TransactionMapper extends Mapper<Object, Text, Text, AggregateWritable> {

		static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		// Map method
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String split[] = value.toString().split(",");

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

	public static class TxAggregator extends Reducer<Text, AggregateWritable, Text, AggregateWritable> {

		public void reduce(Text key, Iterable<AggregateWritable> values, Context context)
				throws IOException, InterruptedException {

			AggregateData aggregateData = new AggregateData();
			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);

			for (AggregateWritable val : values) {
				aggregateData.setOrderabove2000(
						aggregateData.getOrderabove2000() + val.getAggregateData().getOrderabove2000());
				aggregateData.setOrderbelow1000(
						aggregateData.getOrderbelow1000() + val.getAggregateData().getOrderbelow1000());
				aggregateData.setOrderbelow2000(
						aggregateData.getOrderbelow2000() + val.getAggregateData().getOrderbelow2000());
				aggregateData
						.setOrderbelow500(aggregateData.getOrderbelow500() + val.getAggregateData().getOrderbelow500());
				aggregateData.setTotalOrder(val.getAggregateData().getTotalOrder() + aggregateData.getTotalOrder());

			}
			context.write(key, aggregateWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Daily Tx Count Job");
		job.setJarByClass(DailyTransactionCountMRJob.class);
		job.setMapperClass(TransactionMapper.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setCombinerClass(TxAggregator.class);
		job.setReducerClass(TxAggregator.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AggregateWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + Calendar.getInstance().getTimeInMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
