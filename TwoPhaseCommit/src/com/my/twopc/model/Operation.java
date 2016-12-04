package com.my.twopc.model;

public class Operation {

	private String operationType;
	private String fileName;
	private String fileContent;

	public Operation(String operationType, String fileName, String fileContent) {
		this.operationType = operationType;
		this.fileName = fileName;
		this.fileContent = fileContent;
	}

	public String getOperationType() {
		return operationType;
	}
	public void setOperationType(String operationType) {
		this.operationType = operationType;
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
	
}
