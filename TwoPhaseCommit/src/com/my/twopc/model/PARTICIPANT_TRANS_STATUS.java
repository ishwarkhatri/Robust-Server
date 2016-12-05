package com.my.twopc.model;

public enum PARTICIPANT_TRANS_STATUS {
    INCOMING("INCOMING"),
    READY("READY"),
    FAILURE("FAILURE"),
    COMMITTED("COMMITTED"),
    ABORTED("ABORTED");

    private String value;

    PARTICIPANT_TRANS_STATUS(String value) {
        this.value = value;
    }

    public static PARTICIPANT_TRANS_STATUS getEnum(String name) {
        if(name != null)
    	switch(name) {
            case "INCOMING":
                return PARTICIPANT_TRANS_STATUS.INCOMING;

            case "READY":
                return PARTICIPANT_TRANS_STATUS.READY;

            case "FAILURE":
                return PARTICIPANT_TRANS_STATUS.FAILURE;

            case "COMMITTED":
                return PARTICIPANT_TRANS_STATUS.COMMITTED;

            case "ABORTED":
                return PARTICIPANT_TRANS_STATUS.ABORTED;
        }
        return null;
    }

    public String getValue() {
        return value;
    }
}
