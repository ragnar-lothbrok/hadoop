package com.edureka.hadoop.helpers;

import java.io.Serializable;

public class AggregateData implements Serializable {

	private static final long serialVersionUID = 1L;
	private Long orderbelow500 = 0l;
	private Long orderbelow1000 = 0l;
	private Long orderbelow2000 = 0l;
	private Long orderabove2000 = 0l;
	private Long totalOrder = 0l;

	public Long getTotalOrder() {
		return totalOrder;
	}

	public void setTotalOrder(Long totalOrder) {
		this.totalOrder = totalOrder;
	}

	public Long getOrderbelow500() {
		return orderbelow500;
	}

	public void setOrderbelow500(Long orderbelow500) {
		this.orderbelow500 = orderbelow500;
	}

	public Long getOrderbelow1000() {
		return orderbelow1000;
	}

	public void setOrderbelow1000(Long orderbelow1000) {
		this.orderbelow1000 = orderbelow1000;
	}

	public Long getOrderbelow2000() {
		return orderbelow2000;
	}

	public void setOrderbelow2000(Long orderbelow2000) {
		this.orderbelow2000 = orderbelow2000;
	}

	public Long getOrderabove2000() {
		return orderabove2000;
	}

	public void setOrderabove2000(Long orderabove2000) {
		this.orderabove2000 = orderabove2000;
	}

}
