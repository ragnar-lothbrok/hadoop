package com.edureka.hadoop.mr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.hadoop.helpers.AggregateData;
import com.edureka.hadoop.helpers.AggregateWritable;
import com.edureka.hadoop.helpers.Transaction;

public class MerchantAnalyticsJob extends Configured implements Tool {

	private final static Logger LOGGER = LoggerFactory.getLogger(MerchantAnalyticsJob.class);

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static class MerchantPartitioner extends Partitioner<Text, AggregateWritable> {

		@Override
		public int getPartition(Text key, AggregateWritable value, int numPartitions) {
			return Math.abs(key.toString().hashCode()) % numPartitions;
		}
	}

	private static class TransactionMapper extends Mapper<LongWritable, Text, Text, AggregateWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, AggregateWritable>.Context context)
				throws IOException, InterruptedException {

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

			String outputKey = merchantIdNameMap.get(transaction.getMerchantId().toString()) + "-"
					+ (split[3].trim().split(" ")[0].trim());

			context.write(new Text(outputKey), aggregateWritable);
		}

		private static Map<String, String> merchantIdNameMap = new HashMap<String, String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, AggregateWritable>.Context context)
				throws IOException, InterruptedException {
			URI[] paths = context.getCacheArchives();
			if (paths != null) {
				for (URI path : paths) {
					loadMerchantIdNameInCache(path.toString(), context.getConfiguration());
				}
			}
			super.setup(context);
		}

		private void loadMerchantIdNameInCache(String file, Configuration conf) {
			LOGGER.info("File name : " + file);
			String strRead;
			BufferedReader br = null;
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				FSDataInputStream open = fileSystem.open(new Path(file));
				br = new BufferedReader(new InputStreamReader(open));
				while ((strRead = br.readLine()) != null) {
					String line = strRead.toString().replace("\"", "");
					String splitarray[] = line.split(",");
					merchantIdNameMap.put(splitarray[0].toString(), splitarray[2].toString());
				}
			} catch (Exception e) {
				LOGGER.error("exception occured while loading data in cache = {} ", e);
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (Exception e) {
					LOGGER.error("exception occured while closing file reader = {} ", e);
				}
			}
		}
	}

	private static class MerchantOrderReducer extends Reducer<Text, AggregateWritable, Text, AggregateWritable> {
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
		ToolRunner.run(new Configuration(), new MerchantAnalyticsJob(), args);
		System.exit(1);
	}

	private static int runMRJobs(String[] args) throws FileNotFoundException, IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		ControlledJob mrJob1 = new ControlledJob(conf);
		Job job = mrJob1.getJob();

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AggregateWritable.class);
		job.setJarByClass(MerchantAnalyticsJob.class);

		job.setReducerClass(MerchantOrderReducer.class);

		FileInputFormat.setInputDirRecursive(job, true);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransactionMapper.class);
		FileSystem fileSystem = FileSystem.get(job.getConfiguration());
		RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(args[1]), true);
		while (files.hasNext()) {
			job.addCacheArchive(files.next().getPath().toUri());
		}
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + Calendar.getInstance().getTimeInMillis()));
		job.setNumReduceTasks(5);
		job.setPartitionerClass(MerchantPartitioner.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public int run(String[] args) throws Exception {
		return runMRJobs(args);
	}
}
