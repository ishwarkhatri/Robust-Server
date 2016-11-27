package com.my.twopc.model;

public enum COOD_TRANS_STATUS {

	INCOMING("INCOMING"),
	VOTING_STARTED("VOTING_STARTED"),
	COMMITTED("COMMITTED"),
	ABORTED("ABORTED"), 
	REQUEST_PROCESSED("REQUEST_PROCESSED");

	private String value;

	COOD_TRANS_STATUS(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
