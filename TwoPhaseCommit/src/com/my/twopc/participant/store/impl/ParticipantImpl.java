package com.my.twopc.participant.store.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.my.twopc.common.Constants;
import com.my.twopc.coordinator.store.Coordinator;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.PARTICIPANT_TRANS_STATUS;
import com.my.twopc.model.RFile;
import com.my.twopc.model.Status;
import com.my.twopc.model.StatusReport;
import com.my.twopc.model.TempTableDTO;
import com.my.twopc.participant.store.Participant.Iface;

public class ParticipantImpl implements Iface {

	//Variables related to database connection
	private Connection connection;
	private boolean isConnectionAvailable = true;
	private Lock connectionLock = new ReentrantLock();
	private Condition connectionCondition = connectionLock.newCondition();
	private String participantName;

	//Variables related to file locking
	private Lock fileLock = new ReentrantLock();
	private Set<String> lockedFileSet = new HashSet<>();
	private Condition fileLockCondition = fileLock.newCondition();
	private boolean isSetAvailable = true;

	//Coordinator hostname & port
	private String coordinatorHostName;
	private int coordinatorPortNumber;
	private boolean isTestingOn;


	public ParticipantImpl(String coordHostname, int coordPort, String name, boolean isTest) {
		coordinatorHostName = coordHostname;
		coordinatorPortNumber = coordPort;
		participantName = name;
		isTestingOn = isTest;
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

	//Recovery from failure
	private void recoverFromFailure() {
		//Fetch records from TEMP table for which status is not in (COMMITTED, ABORTED)
		List<TempTableDTO> incompleteTransactionList = getPendingTransactions();
		
		//Iterate over all the records fetch in above step
		//For each record
		for(TempTableDTO transaction : incompleteTransactionList) {
			RFile rFile = new RFile(transaction.getTransactionId(), transaction.getFileName(), transaction.getFileContent());
			System.out.println("Incomplete transaction with status: " + transaction.getParticipantStatus().getValue());
			//If status == READY
			if(transaction.getParticipantStatus() == PARTICIPANT_TRANS_STATUS.READY) {
				System.out.println("Getting voting decision from coordinator");
				//1. GET VOTING decision from Coordinator
				PARTICIPANT_TRANS_STATUS votingDecision = getVotingDecisionFor(transaction.getTransactionId());

				//2. IF voting decision was COMMITTED
				if(votingDecision == PARTICIPANT_TRANS_STATUS.COMMITTED) {
					System.out.println("Final decision was COMMIT. Committing file...");
					//	Then copy file name and content in PERMENANT table
					//	And update FINAL_STATUS to COMMITTED and MY_STATUS to READY
					commitFile(rFile);
				}
				else { 
					//	 Else update FINAL_STATUS in TEMP table to ABORTED
					System.out.println("Final decision was ABORT. Aborting...");
					abortFile(rFile);
				}
			}
			//Else If status == FAILURE
			else if(transaction.getParticipantStatus() == PARTICIPANT_TRANS_STATUS.FAILURE) {
				//Update final decision in TEMP table to ABORTED
				System.out.println("Final decision was ABORT. Aborting...");
				abortFile(rFile);
			}
		}
	}

	// Abort file write operation
	private void abortFile(RFile rFile) {
		try {
			abort(rFile.getTid());
		} catch (Exception ouch) {
			printError(ouch, true);
		}
	}

	// Commit file to Permanent table
	private void commitFile(RFile rFile) {
		try {
			commit(rFile.getTid());
		} catch (Exception ouch) {
			printError(ouch, true);
		}
	}

	private PARTICIPANT_TRANS_STATUS getVotingDecisionFor(int transactionId) {
		//Get voting decision from Coordinator for a given transaction id
		try {
			//Connect to the coordinator
			TTransport transport = new TSocket(coordinatorHostName, coordinatorPortNumber);
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			Coordinator.Client coordinator = new Coordinator.Client(protocol);

			String decision = coordinator.getFinalVotingDecision(transactionId);
			if(decision != null)
				return PARTICIPANT_TRANS_STATUS.getEnum(decision);

		}catch(Exception oops) {
			printError(oops, true);
		}
		return null;
	}

	private List<TempTableDTO> getPendingTransactions() {
		List<TempTableDTO> incompleteTransactions = new ArrayList<>();

		connectionLock.lock();
		TempTableDTO tempTableDTO;
		try {
			PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_INCOMPLETE_QUERY);
			connection.commit();

			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				tempTableDTO = new TempTableDTO();
				tempTableDTO.setTransactionId(rs.getInt("TID"));
				tempTableDTO.setFileName(rs.getString("FILE_NAME"));
				tempTableDTO.setFileContent(rs.getString("FILE_CONTENT"));
				tempTableDTO.setParticipantStatus(PARTICIPANT_TRANS_STATUS.getEnum(rs.getString("MY_STATUS")));
				tempTableDTO.setVotingDecision(PARTICIPANT_TRANS_STATUS.getEnum(rs.getString("VOTING_STATUS")));

				incompleteTransactions.add(tempTableDTO);
			}
		} catch (SQLException oops) {
			
		}finally {
			connectionLock.unlock();
		}

		return incompleteTransactions;
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
			connection = DriverManager.getConnection(Constants.TWO_PC_DBNAME + "_" + participantName + ".db");
			connection.setAutoCommit(false);
		}catch(Exception oops) {
			printError(oops, true);
		}
	}

	@Override
	public StatusReport writeToFile(RFile rFile) throws SystemException, TException {
		StatusReport statusRep = new StatusReport(Status.SUCCESSFUL);

		System.out.println("Write request received for '" + rFile.getFilename() + "' contents '" + rFile.getContent() + "'");

		if(isFileLocked(rFile.getFilename())) {
			System.out.println("File '" + rFile.getFilename() + "' with contents '" + rFile.getContent() + "' is locked");
			//Add entry in TEMP table with PARTICIPANT status as ABORTED
			statusRep.setStatus(Status.FAILED);
			statusRep.setMessage("File is locked or being used by another client");

			connectionLock.lock();
			try {
				while(!isConnectionAvailable) {
					try{
						connectionCondition.await();
					}catch(InterruptedException ie){}
				}
				
				isConnectionAvailable = false;
				
				PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_IS_FILE_LOCKED_QUERY);
				ps.setInt(1, rFile.getTid());
				ps.setString(2, rFile.getFilename());
				ps.setString(3, rFile.getContent());
				ps.setString(4, PARTICIPANT_TRANS_STATUS.FAILURE.getValue());
				
				ps.executeUpdate();
				ps.close();
				connection.commit();

				isConnectionAvailable = true;
				connectionCondition.signal();
			} catch (SQLException oops) {
				oops.printStackTrace();
			}finally {
				connectionLock.unlock();
			}
		} else {
			System.out.println("Acquiring lock.");
			//Acquire lock
			boolean isLockAcquired = getLockOnFile(rFile.getFilename());
			System.out.println("Lock acquired on '" + rFile.getFilename() + "' contents '" + rFile.getContent() + "'");

			//Sleep for sometime
			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {}

			//Copy file content to TEMP table
			connectionLock.lock();
			try {
				while(!isConnectionAvailable) {
					try{
						connectionCondition.await();
					}catch(InterruptedException ie){}
				}

				isConnectionAvailable = false;
				
				PreparedStatement ps = null;
				if(isLockAcquired) {
					//if operation is successful then update PARTICIPANT status as READY
					ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_INSERT_QUERY);
					ps.setInt(1, rFile.getTid());
					ps.setString(2, rFile.getFilename());
					ps.setString(3, rFile.getContent());
					ps.setString(4, PARTICIPANT_TRANS_STATUS.READY.getValue());
					ps.executeUpdate();
				} else {
					statusRep.setStatus(Status.FAILED);
					statusRep.setMessage("Failed to acquire lock");

					//else update status as FAILURE
					ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_IS_FILE_LOCKED_QUERY);
					ps.setInt(1, rFile.getTid());
					ps.setString(2, rFile.getFilename());
					ps.setString(3, rFile.getContent());
					ps.setString(4, PARTICIPANT_TRANS_STATUS.FAILURE.getValue());
					ps.executeUpdate();
				}

				ps.close();
				connection.commit();

				isConnectionAvailable = true;
				connectionCondition.signal();
				System.out.println("Write request completed successfully");
			} catch (SQLException oops) {
				System.err.println(oops.getMessage());
				oops.printStackTrace();
			} finally {
				connectionLock.unlock();
			}
			
			//Do not release lock on file here. It should be done in either commit or abort methods

		}
		return statusRep;
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

	private boolean getLockOnFile(String fileName) {
		boolean isLockAcquired = false;
		fileLock.lock();
		try {
			while (!isSetAvailable) {
				try {
					fileLockCondition.await();
				} catch (InterruptedException oops) {}
			}
			isSetAvailable = false;

			if (lockedFileSet.add(fileName))
				isLockAcquired = true;

			isSetAvailable = true;

			fileLockCondition.signal();
		} finally {
			fileLock.unlock();
		}

		return isLockAcquired;
	}

	@Override
	public RFile readFromFile(String filename) throws SystemException, TException {
		//Read the contents of file with name <filename> and return to Co-ordinator
		System.out.println("Read request for file: " + filename);
		RFile requestedFile = null;
		boolean throwFileNotFound = false;
		try {
			PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_PR_TABLE_READ_QUERY);
			ps.setString(1, filename);

			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				requestedFile = new RFile();
				requestedFile.setFilename(filename);
				requestedFile.setContent(rs.getString("FILE_CONTENT"));
				System.out.println("File contents: " + requestedFile.getContent());
			} else {
				System.out.println(filename + " file does not exists");
				throwFileNotFound = true;
			}
		}catch(Exception oops) {
			System.err.println("Could not complete read request for file: " + filename);
			oops.printStackTrace();
			throw new SystemException();
		}
		
		if(throwFileNotFound) {
			SystemException sysEx = new SystemException();
			sysEx.setMessage(Constants.NO_SUCH_FILE_ERR_MSG);
			throw sysEx;
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
		boolean isCommitted = false;
		String fileName = "";
		String content = "";

		if(isTestingOn) {
			System.exit(1);
		}
		// Acquire lock on file
		connectionLock.lock();

		try {
			while (!isConnectionAvailable)
				connectionCondition.await();

			isConnectionAvailable = false;
			//First move file name and content from TEMP table to PERMEMANT table
			PreparedStatement psRead = connection.prepareStatement(Constants.PARTICIPANT_TMP_TABLE_READ_QUERY);
			psRead.setInt(1, tid);

			ResultSet rs = psRead.executeQuery();
			//If the record is present in TMP table
			if (rs.next()) {
				String finalStatus = rs.getString("VOTING_STATUS");
				if(finalStatus == null) {
					
					fileName = rs.getString("FILE_NAME");
					content = rs.getString("FILE_CONTENT");
					
					//Check if the entry is present in PERMANENT table
					psRead = connection.prepareStatement(Constants.PARTICIPANT_PR_TABLE_READ_QUERY);
					psRead.setString(1, fileName);
					rs = psRead.executeQuery();
					
					PreparedStatement psWrite;
					//Overwrite content if file exists
					if(rs.next()) {
						psWrite = connection.prepareStatement(Constants.PARTICIPANT_PR_UPDATE_QUERY);
						psWrite.setString(1, content);
						psWrite.setString(2, fileName);
						psWrite.executeUpdate();
					}
					else { //Else insert new entry
						psWrite = connection.prepareStatement(Constants.PARTICIPANT_PR_INSERT_QUERY);
						psWrite.setString(1, fileName);
						psWrite.setString(2, content);
						psWrite.executeUpdate();
					}
					
					connection.commit();
					
					//Update status for tid in TEMP table to COMMITTED
					PreparedStatement ps = connection.prepareStatement(Constants.PARTICIPANT_TMP_FINAL_STATUS_UPDATE_QUERY);
					ps.setString(1, PARTICIPANT_TRANS_STATUS.COMMITTED.getValue());
					ps.setInt(2, tid);
					ps.executeUpdate();
					connection.commit();
					
				}
				
				//Update the condition variable
				isConnectionAvailable = true;

				//Signal another waiting thread
				connectionCondition.signal();
			}
			//if above steps done successfully then return true
			isCommitted = true;
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
		if(isTestingOn) {
			System.exit(1);
		}
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
			
			//Update only if record is present in database
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
			}

			isCommitted = true;
			isConnectionAvailable = true;
			connectionCondition.signal();
		} catch (Exception oops) {
			printError(oops, true);
		}finally {
			connectionLock.unlock();
		}
		
		//Release lock on file that was acquired in writeFile method
		//if operation done successfully then return true
		if(isCommitted && releaseLock(rFile.getFilename()))
				isSuccessful = true;
		
		return isSuccessful;
	}

	public boolean releaseLock(String filename) {
		fileLock.lock();
		try {
			while(!isSetAvailable) {
				try {
					fileLockCondition.await();
				}catch(InterruptedException oops) {}
			}
			
			isSetAvailable = false;
			
			lockedFileSet.remove(filename);

			isSetAvailable = true;
			fileLockCondition.signal();
		}finally {
			fileLock.unlock();
		}

		return true;
	}

	private void printError(Exception oops, boolean doExit) {
		System.err.println(oops.getClass().getName() + ": " + oops.getMessage());
		if(doExit)
			System.exit(1);
	}
}
