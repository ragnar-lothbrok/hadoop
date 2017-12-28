package com.edureka.hadoop.mr;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edureka.hadoop.helpers.AggregateWritable;
import com.edureka.hadoop.helpers.CustomPartitioner;
import com.edureka.hadoop.helpers.TransactionMapper;
import com.edureka.hadoop.helpers.TxAggregator;

public class DailyTransactionCountMRJob {

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
