package com.edureka.hadoop.custominput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLMRJob {

	private final static Logger LOGGER = LoggerFactory.getLogger(XMLMRJob.class);

	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

	private static class XMLMapper extends Mapper<LongWritable, Text, XMLWritable, CustomWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, XMLWritable, CustomWritable>.Context context)
				throws IOException, InterruptedException {
			OrderData aggregateData = new OrderData();
			XMLWritable xmlWritable = new XMLWritable();
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(new ByteArrayInputStream(value.toString().getBytes()));
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS:
						aggregateData.setTotalOrder(1);
						if (currentElement.equals("L_RECEIPTDATE") && reader.getText().trim().length() > 0) {
							xmlWritable.setRECEIPTDATE(reader.getText());
							Calendar cal = Calendar.getInstance();
							cal.setTime(sdf2.parse(xmlWritable.getRECEIPTDATE()));
							xmlWritable.setMonth(cal.get(Calendar.MONTH));
						}
						if (currentElement.equals("L_EXTENDEDPRICE") && reader.getText().trim().length() > 0) {
							aggregateData.setTotalOrderValue(Double.parseDouble(reader.getText()));
						}
						if (currentElement.equals("L_DISCOUNT") && reader.getText().trim().length() > 0) {
							aggregateData.setTotalDiscountValue(Double.parseDouble(reader.getText()));
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("Exception occured while parsing XML = {} ", e);
			}
			context.write(xmlWritable, new CustomWritable(aggregateData));
		}

	}

	private static class XMLReducer extends Reducer<XMLWritable, CustomWritable, XMLWritable, CustomWritable> {

		@Override
		protected void reduce(XMLWritable arg0, Iterable<CustomWritable> arg1,
				Reducer<XMLWritable, CustomWritable, XMLWritable, CustomWritable>.Context context)
				throws IOException, InterruptedException {
			OrderData orderData = new OrderData();
			for (CustomWritable obj : arg1) {
				orderData.setTotalOrderValue(
						orderData.getTotalOrderValue() + obj.getAggregateData().getTotalOrderValue());
				orderData.setTotalOrder(orderData.getTotalOrder() + obj.getAggregateData().getTotalOrder());
				orderData.setTotalDiscountValue(
						orderData.getTotalDiscountValue() + obj.getAggregateData().getTotalDiscountValue());
			}
			context.write(arg0, new CustomWritable(orderData));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.set("startTag", "<T>");
		conf.set("endTag", "</T>");

		Job job = Job.getInstance(conf, "XMLParseJob");

		job.setMapOutputKeyClass(XMLWritable.class);
		job.setMapOutputValueClass(CustomWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(XMLMapper.class);
		job.setReducerClass(XMLReducer.class);

		job.setInputFormatClass(XMLInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(XMLPartitioner.class);

		if (args.length == 2) {
			Path p = new Path(args[1]);
			FileSystem fs = FileSystem.get(conf);
			fs.exists(p);
			fs.delete(p, true);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + Calendar.getInstance().getTimeInMillis()));
		} else {
			System.exit(0);
		}

		job.waitForCompletion(true);
	}
}