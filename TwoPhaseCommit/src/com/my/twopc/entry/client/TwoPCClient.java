package com.my.twopc.entry.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.my.twopc.coordinator.store.Coordinator;
import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.Operation;
import com.my.twopc.model.RFile;
import com.my.twopc.model.Status;
import com.my.twopc.model.StatusReport;

public class TwoPCClient {
	private static final String OPERATION_FILE = "operations.txt";

	public static void main(String[] args) {
		//Check for valid arguments
		if(args.length < 2) {
			System.err.println("Invalid arguments!\nPlease enter Coordinator's hostname and port number as cmdline arguments");
			System.exit(1);
		}
		
		String coordHostname = args[0];
		int coordPortno = Integer.parseInt(args[1]);
		
		sendMultipleRequests(coordHostname, coordPortno);
	}

	private static void sendMultipleRequests(String coordHostname, int coordPortno) {
		try {
			//Read operations file
			List<Operation> operationList = readOperationFile();


			for(int i = 0; i < operationList.size(); i++) {
				Operation op = operationList.get(i);
				new Thread(new Runnable() {
					
					@Override
					public void run() {
						//Connect to the participant
						TTransport transport = new TSocket(coordHostname, coordPortno);
						try {
							transport.open();
						} catch (TTransportException e) {
							e.printStackTrace();
						}
						
						TProtocol protocol = new TBinaryProtocol(transport);
						Coordinator.Client client = new Coordinator.Client(protocol);
						if("read".equalsIgnoreCase(op.getOperationType())) {
							System.out.println("Reading from file: " + op.getFileName());
							try {
								RFile rfile = client.readFile(op.getFileName());
								System.out.println("Results: " + rfile.getContent());
							}catch(SystemException ouch) {
								System.err.println("Could not complete read request: " + ouch.getMessage());
							} catch (TException e) {
								System.err.println("Unknown exception: " + e.getMessage());
								e.printStackTrace();
							}
						}
						else if("write".equalsIgnoreCase(op.getOperationType())) {
							System.out.println("Writing data to file '" + op.getFileName() + "' contents '" + op.getFileContent() + "'\n");
							try {
								RFile rfile = new RFile(0, op.getFileName(), op.getFileContent());
								StatusReport report = client.writeFile(rfile);
								if(report.getStatus() == Status.SUCCESSFUL) {
									System.out.println("Write request was successfull");
								} else {
									System.err.println("Write request failed: " + report.getMessage());
								}
							}catch(SystemException ouch) {
								System.err.println("Could not complete write request: " + ouch.getMessage());
								ouch.printStackTrace();
							} catch (TException e) {
								System.err.println("Unknown exception: " + e.getMessage());
								e.printStackTrace();
							}
							
						}
						transport.close();
					}
				}).start();
			}
			
		}catch(Exception ouch) {
			System.err.println("Could not send request to coordinator: " + ouch.getMessage());
			ouch.printStackTrace();
			System.exit(1);
		}
	}

	private static List<Operation> readOperationFile() {
		File file = new File(OPERATION_FILE);
		List<Operation> operations = new ArrayList<>();
		try {
			Scanner scan = new Scanner(file);
			while(scan.hasNextLine()) {
				//OPERATION,FILE_NAME,FILE_CONTENT(OPTIONAL)
				String tokens[] = scan.nextLine().split(",");
				Operation op = null;
				if("read".equalsIgnoreCase(tokens[0])) {
					op = new Operation(tokens[0], tokens[1], null);
				} else if("write".equalsIgnoreCase(tokens[0])) {
					op = new Operation(tokens[0], tokens[1], tokens[2]);
				}
				
				operations.add(op);
			}
			scan.close();
		} catch (FileNotFoundException ouch) {
			System.err.println("Could not read operations: " + ouch.getMessage());
			ouch.printStackTrace();
			System.exit(1);
		}
		
		return operations;
	}

}
