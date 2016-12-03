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

	//Variables related to database connection
	private Connection connection;
	private boolean isConnectionAvailable = true;
	private Lock connectionLock = new ReentrantLock();
	private Condition connectionCondition = connectionLock.newCondition();

	//Variables related to file locking
	private Lock fileLock = new ReentrantLock();
	private Set<String> lockedFileSet = new HashSet<>();
	private Condition fileLockCondition = fileLock.newCondition();
	private boolean isSetAvailable = true;

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
				//		Then copy file name and content in PERMENANT table
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
			connectionLock.lock();
			try {
				while(!isConnectionAvailable) {
					try{
						connectionCondition.await();
					}catch(InterruptedException ie){}
				}
				
				PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_IS_FILE_LOCKED_QUERY);
				ps.setInt(1, rFile.getTid());
				ps.setString(2, rFile.getFilename());
				ps.setString(3, rFile.getContent());
				ps.setString(4, PARTICIPANT_TRANS_STATUS.FAILURE.getValue());
				
				ps.executeUpdate();
				ps.close();
				connection.commit();
			} catch (SQLException oops) {
				oops.printStackTrace();
			}
		} else {
			//Acquire lock
			fileLock.lock();

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
		fileLock.lock();
		try {
			while(!isSetAvailable) {
				try {
					fileLockCondition.await();
				}catch(InterruptedException oops) {}
			}
			
			isSetAvailable = false;
			
			if(lockedFileSet.contains(filename))
				isLocked = true;
			
			isSetAvailable = true;
			
			fileLockCondition.signal();
		}finally {
			fileLock.unlock();
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
		//Get PARTICIPANT status for Transaction Id (tid)
		connectionLock.lock();
		String myStatus = PARTICIPANT_TRANS_STATUS.FAILURE.getValue();
		try {
			while(!isConnectionAvailable) {
				try {
					connectionCondition.await();
				}catch(InterruptedException ie){}
			}
			
			isConnectionAvailable = false;
			
			PreparedStatement pst = connection.prepareStatement(Constants.PARTICIPANT_TMP_TABLE_READ_QUERY);
			pst.setInt(1, tid);
			ResultSet rs = pst.executeQuery();
			if(rs.next()) {
				//Vote whatever decision was saved in database
				myStatus = rs.getString("MY_STATUS");
			}

			isConnectionAvailable = true;
			connectionCondition.signal();
		} catch (Exception oops) {
			System.err.println(oops.getMessage());
		}finally {
			connectionLock.unlock();
		}

		//Return status to coordinator
		return myStatus;
	}

	@Override
	public boolean commit(int tid) throws SystemException, TException {
		//TODO COMMIT
		boolean isCommitted = false;
		String fileName = "";
		String content = "";

		// Acquire lock on file
		connectionLock.lock();

		try {
			while (!isConnectionAvailable)
				connectionCondition.await();

			isConnectionAvailable = true;
			//First move file name and content from TEMP table to PERMEMANT table
			PreparedStatement psRead = connection.prepareStatement(Constants.PARTICIPANT_TMP_TABLE_READ_QUERY);
			psRead.setInt(1, tid);

			ResultSet rs = psRead.executeQuery();
			if (rs.next()) {
				fileName = rs.getString("FILE_NAME");
				content = rs.getString("FILE_CONTENT");
			}
			connection.commit();

			PreparedStatement psWrite = connection.prepareStatement(Constants.PARTICIPANT_PR_INSERT_QUERY);
			psWrite.setString(1, fileName);
			psWrite.setString(2, content);
			psWrite.executeUpdate();
			connection.commit();

			//Update status for tid in TEMP table to COMMITTED
			PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_FINAL_STATUS_UPDATE_QUERY);
			ps.setInt(1, tid);

			ps.executeUpdate();
			connection.commit();

			//Update the condition variable
			isConnectionAvailable = true;
			//Signal another waiting thread
			connectionCondition.signal();


			//if above steps done successfully then return true
			isCommitted = true;
			//else return false
		} catch (Exception oops) {
			printError(oops, true);
		}finally {
			// Release the acquired lock
			connectionLock.unlock();
		}
		//Release lock on file that was acquired in writeFile method
		if (isCommitted && releaseLock(fileName))
			return true;
		return false;
	}

	@Override
	public boolean abort(int tid) throws SystemException, TException {
		connectionLock.lock();
		boolean isSuccessful = false;
		boolean isCommitted = false;
		RFile rFile = new RFile();
		try {
			while(!isConnectionAvailable) {
				try {
					connectionCondition.await();
				}catch(InterruptedException ie){}
			}
			isConnectionAvailable = false;
			
			//Read file name for TransactionId = tid
			PreparedStatement pst = connection.prepareStatement(Constants.PARTICIPANT_TMP_TABLE_READ_QUERY);
			pst.setInt(1, tid);
			ResultSet rs = pst.executeQuery();
			if(rs.next()) {
				rFile.setFilename(rs.getString("FILE_NAME"));
				rFile.setContent(rs.getString("FILE_CONTENT"));
				rFile.setTid(rs.getInt("TID"));
				rs.close();
				
				//Update status for tid in TEMP table to ABORTED
				pst = connection.prepareStatement(Constants.PARTICIPANT_TMP_FINAL_STATUS_UPDATE_QUERY);
				pst.setString(1, PARTICIPANT_TRANS_STATUS.ABORTED.getValue());
				pst.setInt(2, tid);
				pst.executeUpdate();
				connection.commit();
				
				isCommitted = true;
			}
			
			isConnectionAvailable = true;
			connectionCondition.signal();
		} catch (Exception oops) {
			System.err.println(oops.getMessage());
		}finally {
			connectionLock.unlock();
		}
		
		//Release lock on file that was acquired in writeFile method
		//if operation done successfully then return true
		if(isCommitted && releaseLock(rFile.getFilename()))
				isSuccessful = true;
		
		return isSuccessful;
	}

	private boolean releaseLock(String filename) {
		boolean isLockReleased = false;
		fileLock.lock();
		try {
			while(!isSetAvailable) {
				try {
					fileLockCondition.await();
				}catch(InterruptedException oops) {}
			}
			
			isSetAvailable = false;
			
			if(lockedFileSet.remove(filename)) {
				isLockReleased = true;
			}

			isSetAvailable = true;
			fileLockCondition.signal();
		}finally {
			fileLock.unlock();
		}

		return isLockReleased;
	}

	private void printError(Exception oops, boolean doExit) {
		System.err.println(oops.getClass().getName() + ": " + oops.getMessage());
		if(doExit)
			System.exit(1);
	}
}
