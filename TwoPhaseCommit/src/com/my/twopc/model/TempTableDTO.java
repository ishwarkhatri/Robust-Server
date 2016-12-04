package com.my.twopc.model;

public class TempTableDTO {

	private int transactionId;
	
	private String fileName;
	
	private String fileContent;
	
	private PARTICIPANT_TRANS_STATUS participantStatus;
	
	private PARTICIPANT_TRANS_STATUS votingDecision;

	public int getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileContent() {
		return fileContent;
	}

	public void setFileContent(String fileContent) {
		this.fileContent = fileContent;
	}

	public PARTICIPANT_TRANS_STATUS getParticipantStatus() {
		return participantStatus;
	}

	public void setParticipantStatus(PARTICIPANT_TRANS_STATUS participantStatus) {
		this.participantStatus = participantStatus;
	}

	public PARTICIPANT_TRANS_STATUS getVotingDecision() {
		return votingDecision;
	}

	public void setVotingDecision(PARTICIPANT_TRANS_STATUS votingDecision) {
		this.votingDecision = votingDecision;
	}
	
}
