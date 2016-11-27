package com.my.twopc.file.store.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.my.twopc.common.Constants;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.file.store.FileStore.Iface;
import com.my.twopc.model.COOD_TRANS_STATUS;
import com.my.twopc.model.RFile;
import com.my.twopc.model.ReplicaInfo;
import com.my.twopc.model.Status;
import com.my.twopc.model.StatusReport;
import com.my.twopc.participant.store.Participant;

public class CoordinatorImpl implements Iface {

	private Connection connection;
	private List<ReplicaInfo> participantList;
	private boolean isConnectionAvailable;
	private Lock connectionLock = new ReentrantLock();
	private Condition condition = connectionLock.newCondition();

	public CoordinatorImpl(List<ReplicaInfo> replicaList) {
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
			ResultSet rs = statement.executeQuery(Constants.COORDINATOR_UNFINISHED_VOTING_RECORDS_QUERY);
			while(rs.next()) {
				String status = rs.getString("STATUS");

				//Switch case based on status
			}
			statement.close();
		}catch(Exception oops) {
			printError(oops, true);
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
			connection = DriverManager.getConnection(Constants.TWO_PC_DBNAME);
			
		}catch(Exception oops) {
			printError(oops, true);
		}
	}

	@Override
	public StatusReport writeFile(RFile rFile) throws SystemException, TException {
		//Log incoming request in transaction table with status as INCOMING
		createTransactionEntry(rFile);

		//Send write request to all replicas/participants
		sendWriteRequestToParticipants(rFile);

		//Update the transaction status from INCOMING to VOTING_STARTED
		updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_STATUS_QUERY, COOD_TRANS_STATUS.VOTING_STARTED);

		//Do voting
		COOD_TRANS_STATUS finalDecision = getDecisionVotesFromParticipants(rFile.getTid());

		//Update final decision either COMMIT or ABORT 
		updateTransaction(rFile.getTid(), Constants.COOD_TRANS_UPDATE_DECISION_QUERY, finalDecision);

		//Based on voting results send COMMIT/ABORT
		sendFinalDecisionToAllParticipants(rFile.getTid(), finalDecision);

		//Update final voting result in transaction table
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
						printError(oops, true);
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
				if(COOD_TRANS_STATUS.ABORTED.getValue().equalsIgnoreCase(decision)) {
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
			while(!isConnectionAvailable)
				condition.await();
			
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
						client.writeToFile(rFile);
						
					}catch(Exception oops) {
						printError(oops, true);
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
			while(!isConnectionAvailable)
				condition.await();
			
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
			try {
				//Connect to the participant
				TTransport transport = new TSocket(replica.getHostname(), replica.getPortno());
				transport.open();
				
				TProtocol protocol = new TBinaryProtocol(transport);
				Participant.Client client = new Participant.Client(protocol);

				//Try to connect to this replica and read file
				RFile file = client.readFromFile(filename);
				return file;
			}catch(Exception e) {
				//If not able to connect, then try to connect to next replica
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
}
