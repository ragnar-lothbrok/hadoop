package com.edureka.hbase.pojo;

import java.io.Serializable;

public class MerchantTransaction implements Serializable {

	private static final long serialVersionUID = 1L;

	private String merchantId;

	private Long orderCount = 0l;

	public String getMerchantId() {
		return merchantId;
	}

	public void setMerchantId(String merchantId) {
		this.merchantId = merchantId;
	}

	public Long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(Long orderCount) {
		this.orderCount = orderCount;
	}

	public MerchantTransaction(String merchantId, Long orderCount) {
		super();
		this.merchantId = merchantId;
		this.orderCount = orderCount;
	}

	public MerchantTransaction() {
	}

	@Override
	public String toString() {
		return "MerchantTransaction [merchantId=" + merchantId + ", orderCount=" + orderCount + "]";
	}

}
