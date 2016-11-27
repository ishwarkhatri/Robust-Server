package com.my.twopc.model;

public class ReplicaInfo {

	public ReplicaInfo(String hostname, int portno) {
		this.hostname = hostname;
		this.portno = portno;
	}

	private String hostname;
	private int portno;

	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public int getPortno() {
		return portno;
	}
	public void setPortno(int portno) {
		this.portno = portno;
	}

}
