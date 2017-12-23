package com.edureka.hadoop.custominput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.gson.Gson;

public class XMLWritable implements WritableComparable<XMLWritable> {

	private static Gson gson = new Gson();

	private String RECEIPTDATE;
	private Integer month;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.RECEIPTDATE);
		out.writeInt(this.month);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.RECEIPTDATE = in.readUTF();
		this.month = in.readInt();
	}

	public String getRECEIPTDATE() {
		return RECEIPTDATE;
	}

	public void setRECEIPTDATE(String rECEIPTDATE) {
		RECEIPTDATE = rECEIPTDATE;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	@Override
	public int compareTo(XMLWritable o) {
		int result = this.month.compareTo(o.month);
		if (result == 0) {
			return this.RECEIPTDATE.compareTo(o.RECEIPTDATE);
		}
		return result;
	}

	@Override
	public String toString() {
		return gson.toJson(this);
	}
}