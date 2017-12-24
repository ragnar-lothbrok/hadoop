package com.edureka.hbase.pojo;

import java.io.Serializable;

public class SegmentOrder implements Serializable {

	private static final long serialVersionUID = 1L;

	private String segment;
	private Long orderCount = 0l;

	public String getSegment() {
		return segment;
	}

	public void setSegment(String segment) {
		this.segment = segment;
	}

	public Long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(Long orderCount) {
		this.orderCount = orderCount;
	}

	public SegmentOrder() {
	}

	public SegmentOrder(String segment, Long orderCount) {
		super();
		this.segment = segment;
		this.orderCount = orderCount;
	}

	@Override
	public String toString() {
		return "SegmentOrder [segment=" + segment + ", orderCount=" + orderCount + "]";
	}

}
