package com.my.twopc.coordinator.store.impl;

import com.my.twopc.common.Constants;
import com.my.twopc.coordinator.store.Coordinator.Iface;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.*;
import com.my.twopc.participant.store.Participant;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.sql.*;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CoordinatorImpl implements Iface {

	private Connection connection;
	private List<ReplicaInfo> participantList;
	private boolean isConnectionAvailable = true;
	private Lock connectionLock = new ReentrantLock();
	private Condition condition = connectionLock.newCondition();
	private boolean isExitBeforeVoting;
	private boolean isExitAfterVoting;

	public CoordinatorImpl(List<ReplicaInfo> replicaList, String testNo) {
		if("TEST3".equalsIgnoreCase(testNo)) {
    		isExitBeforeVoting = true;
    	} else if("TEST4".equalsIgnoreCase(testNo)) {
    		isExitAfterVoting = true;
    	}
		participantList = replicaList;
		//Initialize the state of co-ordinator
		//Recover from failure if any, check for incomplete requests
		init();
	}

	private void init() {
		//Create/Open sql database
		createOpenSqlDb();

		//Create table for transaction logs
		createTransactionLogTable();

		//Recover from crash (If failed before)
		recoverFromFailure();

	}

	private void recoverFromFailure() {
		Statement statement = null;
		try {
			statement = connection.createStatement();

			//Fetch records for which voting was not finished
			ResultSet rs = statement.executeQuery(Constants.COORDINATOR_UNFINISHED_RECORDS_QUERY);
			while(rs.next()) {
				String status = rs.getString("STATUS");

				// Create RFile from table
				RFile rFile = new RFile();
				rFile.setTid(rs.getInt("TID"));
				rFile.setFilename(rs.getString("FILE_NAME"));
				rFile.setContent(rs.getString("FILE_CONTENT"));

				COOD_TRANS_STATUS finalDecision;

				System.out.println("Incomplete transaction with status '" + status + "', name '" + rFile.getFilename() + "' and content '" + rFile.getContent() + "'");
				//Switch case based on status
				switch (status) {
					case "INCOMING":
						//Ask participants to release lock if acquired
						//sendReleaseLockRequests(rFile);

						//Send write request to all replicas/participants
						//sendWriteRequestToParticipants(rFile);

						//Wait for completion of all write requests
//						try {
//							Thread.sleep(3000);
//						}catch(InterruptedException ie){}
						
						//Update the transaction status from INCOMING to VOTING_STARTED
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.VOTING_STARTED);

						//Do voting
						System.out.println("Starting revote...");
						finalDecision = getDecisionVotesFromParticipants(rFile.getTid());

						//Update final decision either COMMIT or ABORT
						System.out.println("Final decision: " + finalDecision.getValue());
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_DECISION_QUERY, finalDecision);

						//Based on voting results send COMMIT/ABORT
						System.out.println("Sending " + finalDecision.getValue() + " to all participants");
						sendFinalDecisionToAllParticipants(rFile.getTid(), finalDecision);

						//Update final voting result in transaction table
						System.out.println("Marking request as Completed");
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.REQUEST_PROCESSED);

						break;

					case "VOTING_STARTED":
						//Do voting
						System.out.println("Starting revote...");
						finalDecision = getDecisionVotesFromParticipants(rFile.getTid());

						//Update final decision either COMMIT or ABORT
						System.out.println("Final decision: " + finalDecision.getValue());
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_DECISION_QUERY, finalDecision);

						//Based on voting results send COMMIT/ABORT
						System.out.println("Sending " + finalDecision.getValue() + " to all participants");
						sendFinalDecisionToAllParticipants(rFile.getTid(), finalDecision);

						//Update final voting result in transaction table
						System.out.println("Marking request as Completed");
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.REQUEST_PROCESSED);
						break;

					case "COMMITTED":
					case "ABORTED":
						finalDecision = COOD_TRANS_STATUS.getEnum(rs.getString("FINAL_DECISION"));
						System.out.println("Final decision: " + finalDecision.getValue());
						
						//Based on voting results send COMMIT/ABORT
						System.out.println("Sending " + finalDecision.getValue() + " to all participants");
						sendFinalDecisionToAllParticipants(rFile.getTid(), finalDecision);

						//Update final voting result in transaction table
						System.out.println("Marking request as Completed");
						updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.REQUEST_PROCESSED);

						break;
				}
			}
			statement.close();
		}catch(Exception oops) {
			printError(oops, true);
		}
	}

	private void sendReleaseLockRequests(RFile rFile) {
		for(ReplicaInfo replica : participantList) {
			try {
				//Connect to the participant
				TTransport transport = new TSocket(replica.getHostname(), replica.getPortno());
				transport.open();
				
				TProtocol protocol = new TBinaryProtocol(transport);
				Participant.Client client = new Participant.Client(protocol);

				//Send release lock request to participant
				client.releaseLock(rFile.getFilename());

			}catch(Exception oops) {
				System.err.println("Error while sending write request");
				printError(oops, false);
			}
		}
	}

	private void createTransactionLogTable() {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.executeUpdate(Constants.COORDINATOR_CREATE_LOG_TABLE_QUERY);
			statement.close();
		}catch(Exception oops) {
			printError(oops, true);
		}
	}


	private void createOpenSqlDb() {
		try {
			Class.forName(Constants.JDBC_CONNECTION);
			connection = DriverManager.getConnection(Constants.COORD_TWO_PC_DBNAME);
			connection.setAutoCommit(false);
		}catch(Exception oops) {
			printError(oops, true);
		}
	}

	@Override
	public StatusReport writeFile(RFile rFile) throws SystemException, TException {
		//Log incoming request in transaction table with status as INCOMING
		System.out.println("Write request received for '" + rFile.getFilename() + "' contents '" + rFile.getContent() + "'");
		
		createTransactionEntry(rFile);

		//Send write request to all replicas/participants
		System.out.println("Sending write request to all participants");
		sendWriteRequestToParticipants(rFile);

		try {
			System.out.println("Waiting for completion of all write requests");
			Thread.sleep(3000);
		} catch (InterruptedException e) {}

		if(isExitBeforeVoting) {
			System.exit(1);
		}

		//Update the transaction status from INCOMING to VOTING_STARTED
		updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.VOTING_STARTED);

		//Do voting
		System.out.println("Starting voting");
		COOD_TRANS_STATUS finalDecision = getDecisionVotesFromParticipants(rFile.getTid());

		if(isExitAfterVoting) {
			System.exit(1);
		}

		//Update final decision either COMMIT or ABORT 
		System.out.println("Updating final decision for file: " + rFile.getFilename() + ": " + finalDecision.getValue());
		updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_DECISION_QUERY, finalDecision);

		//Based on voting results send COMMIT/ABORT
		System.out.println("Sending final decision to all participants");
		sendFinalDecisionToAllParticipants(rFile.getTid(), finalDecision);

		//Update final voting result in transaction table
		System.out.println("Marking request as processed");
		updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.REQUEST_PROCESSED);

		if(finalDecision == COOD_TRANS_STATUS.COMMITTED)
			return new StatusReport(Status.SUCCESSFUL);

		StatusReport status = new StatusReport(Status.FAILED);
		status.setMessage("Could not complete the operation due to technical problem. Please try again later.");
		return status;
	}

	private void sendFinalDecisionToAllParticipants(int tId, COOD_TRANS_STATUS finalDecision) {
		for(ReplicaInfo replica : participantList) {
			//Create separate thread for sending decisions to save time
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						//Connect to the participant
						TTransport transport = new TSocket(replica.getHostname(), replica.getPortno());
						transport.open();
						
						TProtocol protocol = new TBinaryProtocol(transport);
						Participant.Client client = new Participant.Client(protocol);

						//Depending on the final decision send either commit or abort
						if(finalDecision == COOD_TRANS_STATUS.ABORTED)
							client.abort(tId);
						else if(finalDecision == COOD_TRANS_STATUS.COMMITTED)
							client.commit(tId);

					}catch(Exception oops) {
						printError(oops, false);
					}
				}
			}).start();
		}
	}

	private COOD_TRANS_STATUS getDecisionVotesFromParticipants(int tId) {
		boolean isAborted = false;
		//Get votes from all participants/replicas
		for(ReplicaInfo replica : participantList) {
			try {
				//Connect to the participant
				TTransport transport = new TSocket(replica.getHostname(), replica.getPortno());
				transport.open();
				
				TProtocol protocol = new TBinaryProtocol(transport);
				Participant.Client client = new Participant.Client(protocol);

				//Get decision on particular transaction id which is unique
				String decision = client.vote(tId);

				//If decision is abort for any participant then abort
				if(PARTICIPANT_TRANS_STATUS.FAILURE.getValue().equalsIgnoreCase(decision)) {
					isAborted = true;
					break;
				}
				
			}catch(Exception oops) {
				printError(oops, true);
			}
		}

		//Return status
		if(isAborted)
			return COOD_TRANS_STATUS.ABORTED;

		return COOD_TRANS_STATUS.COMMITTED;
	}

	/**
	 * Update the status of log entry for a given transaction id to given new status
	 * @param tId Transaction id to identify a record
	 * @param newStatus New status to be updated
	 */
	private void updateTransaction(int tId, String updateQuery, COOD_TRANS_STATUS newStatus) {
		connectionLock.lock(); //do not forget to unlock this in finally
		try {
			while(!isConnectionAvailable) {
				try {
					condition.await();
				}catch(InterruptedException ie){}
			}
			
			isConnectionAvailable = false;
			PreparedStatement ps = connection.prepareStatement(updateQuery);
			ps.setString(1, newStatus.getValue());
			ps.setInt(2, tId);

			//Execute and commit the query
			ps.executeUpdate();
			connection.commit();
			
			//Update the condition variable
			isConnectionAvailable = true;
			//Signal another waiting thread
			condition.signal();
		}catch(Exception oops) {
			printError(oops, true);
		}finally {
			connectionLock.unlock();
		}
	}

	/**
	 * Create a separate thread to send write requests to all participants concurrently
	 * This way each participant will start working on request concurrently
	 */
	private void sendWriteRequestToParticipants(RFile rFile) {
		for(ReplicaInfo replica : participantList) {
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						//Connect to the participant
						TTransport transport = new TSocket(replica.getHostname(), replica.getPortno());
						transport.open();
						
						TProtocol protocol = new TBinaryProtocol(transport);
						Participant.Client client = new Participant.Client(protocol);

						//Send write request to participant
						System.out.println("Sending write request to " + replica.getHostname() + " file: " + rFile.getFilename());
						client.writeToFile(rFile);
						System.out.println("Request sent successfully");

					}catch(Exception oops) {
						System.err.println("Error while sending write request");
						printError(oops, false);
					}
				}
			}).start();
		}
	}

	/**
	 * Create new entry log in database for incoming request
	 */
	private void createTransactionEntry(RFile rFile) {
		String fileName = rFile.getFilename();
		String content = rFile.getContent();
		connectionLock.lock();
		try {
			while(!isConnectionAvailable) {
				try {
					condition.await();
				}catch(InterruptedException ie){}
			}
			
			isConnectionAvailable = false;
			PreparedStatement ps = connection.prepareStatement(Constants.COOD_TRANS_INSERT_QUERY, Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, fileName);
			ps.setString(2, content);
			ps.setString(3, COOD_TRANS_STATUS.INCOMING.getValue());

			if(ps.executeUpdate() != 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if(rs.next()) {
					rFile.setTid(rs.getInt(1));
				}
			}
			connection.commit();

			isConnectionAvailable = true;
			condition.signal();
		}catch(Exception e) {
			printError(e, true);
		}finally {
			connectionLock.unlock();
		}
		
	}

	@Override
	public RFile readFile(String filename) throws SystemException, TException {
		for(ReplicaInfo replica : participantList) {
			TTransport transport;
			try {
				//Connect to the participant
				transport = new TSocket(replica.getHostname(), replica.getPortno());
				transport.open();
				
			}catch(Exception ouch) {
				//If not able to connect, then try to connect to next replica
				System.err.println("Could not connect to participant " + replica.getHostname() + ": " + ouch.getMessage());
				ouch.printStackTrace();
				System.err.println("Will try connecting others...");
				continue;
			}

			TProtocol protocol = new TBinaryProtocol(transport);
			Participant.Client client = new Participant.Client(protocol);

			//Try to connect to this replica and read file
			try {
				RFile file = client.readFromFile(filename);
				return file;
			}catch(SystemException ouch) {
				System.err.println(ouch.getMessage());
				throw ouch;
			}
		}

		//If no participant is active then throw exception
		SystemException sysEx = new SystemException();
		sysEx.setMessage("Could not retrieve data right now.. All participants not responding. Retry after sometime.");
		throw sysEx;
	}

	private void printError(Exception oops, boolean doExit) {
		System.err.println(oops.getClass().getName() + ": " + oops.getMessage());
		if(doExit)
			System.exit(1);
	}

	@Override
	public String getFinalVotingDecision(int tid) throws SystemException, TException {
		// TODO Get final voting decision taken
		connectionLock.lock();
		try {
			while(!isConnectionAvailable) {
				try {
					condition.await();
				}catch(InterruptedException ie){}
			}
			
			isConnectionAvailable = false;
			PreparedStatement ps = connection.prepareStatement(Constants.COOD_TRANS_GET_DECISION_QUERY);
			ps.setInt(1, tid);
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				return rs.getString(1);
			}
			
			isConnectionAvailable = true;
			condition.signal();
		}catch(SQLException ouch) {
			printError(ouch, false);
		}
		finally {
			connectionLock.unlock();
		}
		return null;
	}
}
