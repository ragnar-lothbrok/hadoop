package com.edureka.hadoop.helpers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, AggregateWritable> {

	@Override
	public int getPartition(Text arg0, AggregateWritable arg1, int reducerTasks) {
		return arg0.hashCode() % reducerTasks;
	}

}
