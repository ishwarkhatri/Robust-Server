package com.my.twopc.participant.store.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.my.twopc.common.Constants;
import com.my.twopc.model.COOD_TRANS_STATUS;

public class Test {

	private static Connection conn = null;

	public static void main(String[] args) throws Exception {
		Class.forName(Constants.JDBC_CONNECTION);
		conn = DriverManager.getConnection(Constants.TWO_PC_DBNAME);
		
		insert();
		
		select();
	}

	private static void select() throws Exception {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(Constants.COORDINATOR_UNFINISHED_RECORDS_QUERY);
		while(rs.next()){
			printResults(rs);
		}
	}

	private static void printResults(ResultSet rs) throws SQLException {
		System.out.println(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4) + " " + rs.getString(5) + "\n");
	}

	private static void insert() throws Exception {
		PreparedStatement stmt = conn.prepareStatement(Constants.COOD_TRANS_INSERT_QUERY);
		stmt.setString(1, "File 1");
		stmt.setString(2, "Hello World!");
		stmt.setString(3, COOD_TRANS_STATUS.INCOMING.getValue());
		
		stmt.executeUpdate();
	}

}
