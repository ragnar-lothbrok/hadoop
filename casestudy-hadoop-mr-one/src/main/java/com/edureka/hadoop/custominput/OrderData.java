package com.edureka.hadoop.custominput;

import java.io.Serializable;

public class OrderData implements Serializable {

	private static final long serialVersionUID = 1L;
	private double totalOrder = 0.0;
	private double totalOrderValue = 0.0;
	private double totalDiscountValue = 0.0;

	public double getTotalOrder() {
		return totalOrder;
	}

	public void setTotalOrder(double totalOrder) {
		this.totalOrder = totalOrder;
	}

	public double getTotalOrderValue() {
		return totalOrderValue;
	}

	public void setTotalOrderValue(double totalOrderValue) {
		this.totalOrderValue = totalOrderValue;
	}

	public double getTotalDiscountValue() {
		return totalDiscountValue;
	}

	public void setTotalDiscountValue(double totalDiscountValue) {
		this.totalDiscountValue = totalDiscountValue;
	}

	@Override
	public String toString() {
		return "OrderData [totalOrder=" + totalOrder + ", totalOrderValue=" + totalOrderValue + ", totalDiscountValue="
				+ totalDiscountValue + "]";
	}

}
