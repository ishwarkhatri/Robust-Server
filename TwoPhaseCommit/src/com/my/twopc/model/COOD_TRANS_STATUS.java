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
	
	public static COOD_TRANS_STATUS getEnum(String name) {
		switch(name) {
		
		case "INCOMING" :
				return COOD_TRANS_STATUS.INCOMING;
		case "VOTING_STARTED" :
				return COOD_TRANS_STATUS.VOTING_STARTED;
		case "COMMITTED" :
				return COOD_TRANS_STATUS.COMMITTED;
		case "ABORTED" :
				return COOD_TRANS_STATUS.ABORTED;
		case "REQUEST_PROCESSED" :
				return COOD_TRANS_STATUS.REQUEST_PROCESSED;
			
		}
		return null;
	}
}
