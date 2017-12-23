package com.edureka.hadoop.custominput;

import org.apache.hadoop.mapreduce.Partitioner;

public class XMLPartitioner extends Partitioner<XMLWritable, CustomWritable> {

	@Override
	public int getPartition(XMLWritable key, CustomWritable value, int numPartitions) {
		return key.getMonth() % numPartitions;
	}

}
