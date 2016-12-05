package com.my.twopc.common;

import com.my.twopc.model.COOD_TRANS_STATUS;

public class Constants {

	/**
	 * 
	 */
	public static final String JDBC_CONNECTION = "org.sqlite.JDBC";
	public static final String TWO_PC_DBNAME = "jdbc:sqlite:participanttwophasecommit";
	public static final String COORD_TWO_PC_DBNAME = "jdbc:sqlite:coordinatortwophasecommit.db";
	
	/**
	 * Coordinator related queries
	 */
	public static final String COOD_TRANS_TABLE = "COOD_TRANS_LOG";
	public static final String COORDINATOR_CREATE_LOG_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS " + COOD_TRANS_TABLE 
																	+ " (TID INTEGER PRIMARY KEY AUTOINCREMENT,"
																	+ " FILE_NAME CHAR(50),"
																	+ " FILE_CONTENT TEXT,"
																	+ " STATUS CHAR(50),"
																	+ " FINAL_DECISION CHAR(50))";
	public static final String COORDINATOR_UNFINISHED_RECORDS_QUERY = "SELECT * FROM " + COOD_TRANS_TABLE + " where STATUS <> \"" + COOD_TRANS_STATUS.REQUEST_PROCESSED.getValue() + "\";";
	public static final String COOD_TRANS_INSERT_QUERY = "INSERT INTO " + COOD_TRANS_TABLE + " (FILE_NAME, FILE_CONTENT, STATUS) VALUES (?, ?, ?);";
	public static final String COOD_TRANS_UPDATE_STATUS_QUERY = "UPDATE " + COOD_TRANS_TABLE + " SET STATUS = ? WHERE TID = ?";
	public static final String COOD_TRANS_UPDATE_DECISION_QUERY = "UPDATE " + COOD_TRANS_TABLE + " SET FINAL_DECISION = ? WHERE TID = ?";
	public static final String COOD_TRANS_GET_DECISION_QUERY = "SELECT FINAL_DECISION FROM " + COOD_TRANS_TABLE + " where TID =?";

	public static final String STATUS_INCOMING = "INCOMING";
	public static final String STATUS_STARTED = "VOTING_STARTED";
	public static final String STATUS_COMMITTED = "COMMITTED";
	public static final String STATUS_ABORTED = "ABORTED";
	public static final String NO_SUCH_FILE_ERR_MSG = "The requested file does not exist";
	
	
	/**
	 * Participant related queries
	 */
	public static final String PARTICIPANT_PR_TABLE_NAME = "PARTICIPANT_FILE_DATABASE";
	public static final String PARTICIPANT_TMP_TABLE_NAME = "PARTICIPANT_TEMP_DATABASE";
	public static final String PARTICIPANT_CREATE_PR_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS " + PARTICIPANT_PR_TABLE_NAME
																	+ " (FILE_NAME CHAR(50) PRIMARY KEY,"
																	+ " FILE_CONTENT TEXT)";
	public static final String PARTICIPANT_CREATE_TMP_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS " + PARTICIPANT_TMP_TABLE_NAME
																	+ " (TID INTEGER PRIMARY KEY,"
																	+ " FILE_NAME CHAR(50),"
																	+ " FILE_CONTENT TEXT,"
																	+ " MY_STATUS CHAR(50),"
																	+ " VOTING_STATUS CHAR(50))";
	public static final String PARTICIPANT_PR_TABLE_READ_QUERY = "SELECT * FROM " + PARTICIPANT_PR_TABLE_NAME + " WHERE FILE_NAME = ?";
	public static final String PARTICIPANT_PR_INSERT_QUERY = "INSERT INTO " + PARTICIPANT_PR_TABLE_NAME + " (FILE_NAME, FILE_CONTENT) VALUES(?, ?);";
    public static final String PARTICIPANT_PR_UPDATE_QUERY = "UPDATE " + PARTICIPANT_PR_TABLE_NAME + " SET FILE_CONTENT = ? WHERE FILE_NAME = ?";
	public static final String PARTICIPANT_TMP_TABLE_READ_QUERY = "SELECT * FROM " + PARTICIPANT_TMP_TABLE_NAME + " WHERE TID = ?";
	public static final String PARTICIPANT_TMP_INSERT_QUERY = "INSERT INTO " + PARTICIPANT_TMP_TABLE_NAME + " (TID, FILE_NAME, FILE_CONTENT, MY_STATUS) VALUES (?, ?, ?, ?);";
	public static final String PARTICIPANT_TMP_SELF_STATUS_UPDATE_QUERY = "UPDATE " + PARTICIPANT_TMP_TABLE_NAME + " SET MY_STATUS = ? WHERE TID = ?";
	public static final String PARTICIPANT_TMP_FINAL_STATUS_UPDATE_QUERY = "UPDATE " + PARTICIPANT_TMP_TABLE_NAME + " SET VOTING_STATUS = ? WHERE TID = ?";
	public static final String PARTICIPANT_TMP_IS_FILE_LOCKED_QUERY = "INSERT INTO " + PARTICIPANT_TMP_TABLE_NAME + " (TID, FILE_NAME, FILE_CONTENT, MY_STATUS) VALUES (?, ?, ?, ?);";
	public static final String PARTICIPANT_TMP_INCOMPLETE_QUERY = "SELECT * FROM " + PARTICIPANT_TMP_TABLE_NAME + " WHERE VOTING_STATUS IS NULL";

}
