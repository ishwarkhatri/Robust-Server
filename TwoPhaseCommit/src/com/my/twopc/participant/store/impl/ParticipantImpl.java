package com.my.twopc.participant.store.impl;

import com.my.twopc.common.Constants;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.PARTICIPANT_TRANS_STATUS;
import com.my.twopc.model.RFile;
import com.my.twopc.model.StatusReport;
import com.my.twopc.participant.store.Participant.Iface;
import org.apache.thrift.TException;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ParticipantImpl implements Iface {

	private Connection connection;
	private static final Lock FILE_LOCK = new ReentrantLock();
	private static final Set<String> LOCKED_FILES = new HashSet<>();
	private static final Condition LOCK_CONDITION = FILE_LOCK.newCondition();
	private static boolean isSetAvailable = true;

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
		//TODO Recovery from failure
		//Fetch records from TEMP table for which status is not in (COMMITTED, ABORTED)
		
		//Iterate over all the records fetch in above step
		//For each record
			//If status == READY
				//1. GET VOTING decision from Coordinator
				//2. IF voting decision was COMMITTED
				//		Then copy file name and content in PERMANENT table
				//		Update status to COMMITTED and PARTICIPANT status READY
				//	 Else update final decision in TEMP table to ABORTED
			//Else If status == FAILURE
				//Update final decision in TEMP table to ABORTED
		
	}

	private void createTransactionLogTable() {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.executeUpdate(Constants.PARTICIPANT_CREATE_PR_TABLE_QUERY);
			statement.close();
			
			statement = connection.createStatement();
			statement.executeUpdate(Constants.PARTICIPANT_CREATE_TMP_TABLE_QUERY);
			statement.close();
		}catch(Exception oops) {
			printError(oops, true);
		}
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
		//TODO write to file method
		if(isFileLocked(rFile.getFilename())) {
			//Add entry in TEMP table with PARTICIPANT status as ABORTED
			try {
				PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_SELF_STATUS_UPDATE_QUERY);
				ps.setString(1, PARTICIPANT_TRANS_STATUS.ABORTED.toString());
				ps.setInt(2, rFile.getTid());

				ps.close();
				connection.commit();
			} catch (SQLException oops) {
				oops.printStackTrace();
			}
		} else {
			//Acquire lock
			FILE_LOCK.lock();

			//Copy file content to TEMP table
			try {
				PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_INSERT_QUERY);
				ps.setInt(1, rFile.getTid());
				ps.setString(2, rFile.getFilename());
				ps.setString(3, rFile.getContent());

				ps.close();
			} catch (SQLException oops) {

			}
			//if operation is successful then update PARTICIPANT status as READY
			//else update status as FAILURE and final decision as ABORTED

			
			//Do not release lock here. It should be done in either commit or abort methods
		}
		return null;
	}

	private boolean isFileLocked(String filename) {
		boolean isLocked = false;
		FILE_LOCK.lock();
		try {
			while(!isSetAvailable) {
				try {
					LOCK_CONDITION.await();
				}catch(InterruptedException oops) {}
			}
			
			isSetAvailable = false;
			
			if(LOCKED_FILES.contains(filename))
				isLocked = true;
			
			isSetAvailable = true;
			
			LOCK_CONDITION.signal();
		}finally {
			FILE_LOCK.unlock();
		}

		return isLocked;
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
		//TODO Voting method
		//Get PARTICIPANT status for Transaction Id (tid) 
		//Return status to coordinator
		return null;
	}

	@Override
	public boolean commit(int tid) throws SystemException, TException {
		//TODO COMMIT
		//First move file name and content from TEMP table to PERMANENT table
		//Update status for tid in TEMP table to COMMITTED
		//Release lock on file that was acquired in writeFile method
		//if above steps done successfully then return true
		//else return false
		return false;
	}

	@Override
	public boolean abort(int tid) throws SystemException, TException {
		//TODO ABORT
		//Update status for tid in TEMP table to ABORTED
		//Release lock on file that was acquired in writeFile method
		//if operation done successfully then return true
		//else false
		return false;
	}

	private void printError(Exception oops, boolean doExit) {
		System.err.println(oops.getClass().getName() + ": " + oops.getMessage());
		if(doExit)
			System.exit(1);
	}
}
