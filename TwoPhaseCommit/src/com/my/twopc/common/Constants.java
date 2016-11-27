package com.my.twopc.common;

import com.my.twopc.model.COOD_TRANS_STATUS;

public class Constants {

	public static final String JDBC_CONNECTION = "org.sqlite.JDBC";
	public static final String TWO_PC_DBNAME = "jdbc:sqlite:twophasecommit.db";
	public static final String COOD_TRANS_TABLE = "COOD_TRANS_LOG";
	public static final String COORDINATOR_CREATE_LOG_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS " + COOD_TRANS_TABLE 
																	+ " (TID INTEGER PRIMARY KEY AUTOINCREMENT,"
																	+ " FILE_NAME CHAR(50),"
																	+ " FILE_CONTENT TEXT,"
																	+ " STATUS CHAR(50))"
																	+ " FINAL_DECISION CHAR(50)";
	public static final String COORDINATOR_UNFINISHED_VOTING_RECORDS_QUERY = "SELECT * FROM " + COOD_TRANS_TABLE + " where STATUS <> \"" + COOD_TRANS_STATUS.REQUEST_PROCESSED.getValue() + "\";";
	public static final String COOD_TRANS_INSERT_QUERY = "INSERT INTO " + COOD_TRANS_TABLE + " (FILE_NAME, FILE_CONTENT, STATUS) VALUES (?, ?, ?);";
	public static final String COOD_TRANS_UPDATE_STATUS_QUERY = "UPDATE " + COOD_TRANS_TABLE + " SET STATUS = ? WHERE TID = ?";
	public static final String COOD_TRANS_UPDATE_DECISION_QUERY = "UPDATE " + COOD_TRANS_TABLE + " SET FINAL_DECISION = ? WHERE TID = ?";

	public static final String STATUS_INCOMING = "INCOMING";
	public static final String STATUS_STARTED = "VOTING_STARTED";
	public static final String STATUS_COMMITTED = "COMMITTED";
	public static final String STATUS_ABORTED = "ABORTED";
}
