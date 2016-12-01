package com.my.twopc.participant.store.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.thrift.TException;

import com.my.twopc.common.Constants;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.RFile;
import com.my.twopc.model.StatusReport;
import com.my.twopc.participant.store.Participant.Iface;

public class ParticipantImpl implements Iface {

	private Connection connection;

	public ParticipantImpl() {
		initParticipant();
	}

	private void initParticipant() {
		//Create/Open sql database
		createOpenSqlDb();

		//Create table for transaction logs
		createTransactionLogTable();

		//Recover from crash (If failed before)
		recoverFromFailure();
	}

	private void recoverFromFailure() {
		
	}

	private void createTransactionLogTable() {
		
	}

	private void createOpenSqlDb() {
		try {
			Class.forName(Constants.JDBC_CONNECTION);
			connection = DriverManager.getConnection(Constants.TWO_PC_DBNAME);
		}catch(Exception oops) {
			printError(oops, true);
		}
	}

	@Override
	public StatusReport writeToFile(RFile rFile) throws SystemException, TException {
		return null;
	}

	@Override
	public RFile readFromFile(String filename) throws SystemException, TException {
		//Read the contents of file with name <filename> and return to Co-ordinator
		RFile requestedFile = new RFile();
		try {
			PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_PR_TABLE_READ_QUERY);
			ps.setString(1, filename);
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				requestedFile.setFilename(filename);
				requestedFile.setContent(rs.getString("FILE_CONTENT"));
			}else {
				SystemException sysEx = new SystemException();
				sysEx.setMessage(Constants.NO_SUCH_FILE_ERR_MSG);
				throw sysEx;
			}
		}catch(Exception oops) {
			
		}
		return requestedFile;
	}

	@Override
	public String vote(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean commit(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean abort(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	private void printError(Exception oops, boolean doExit) {
		System.err.println(oops.getClass().getName() + ": " + oops.getMessage());
		if(doExit)
			System.exit(1);
	}
}
