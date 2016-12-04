package com.my.twopc.entry.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.my.twopc.participant.store.Participant;
import com.my.twopc.participant.store.Participant.Iface;
import com.my.twopc.participant.store.impl.ParticipantImpl;

public class ParticipantServer {

	public static void main(String[] args) {
		//Check for valid arguments
		if(args.length < 3) {
			System.err.println("Invalid arguments!\n<PARTICIPANT_PORT_NO> <COORDINATOR_HOST_NAME> <COORDINATOR_PORT_NO>");
			System.exit(1);
		}
		
		int myPortNo = Integer.parseInt(args[0]);
		String coordHostname = args[1];
		int coordPortno = Integer.parseInt(args[2]);
		
		startAndServe(coordHostname, coordPortno, myPortNo);
	}

	private static void startAndServe(String coordHostname, int coordPortno, int myPortNo) {
		try {
			//Create server socket
			TServerSocket serverTransport = new TServerSocket(myPortNo);

			//Initialize implementor object
			//This implementor will recover any incomplete transactions from last crash
			ParticipantImpl participantImpl = new ParticipantImpl(coordHostname, coordPortno);

			//Create processor object with above implementor
			Participant.Processor<Iface> processor = new Participant.Processor<Participant.Iface>(participantImpl);

			//Run in a separate deamon thread
			Runnable simple = new Runnable() {
                public void run() {
                	//Create a multi-threaded server
                	TServer multiThreadedParticipantServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
                	
                	System.out.println("Participant Address");
                	try {
						System.out.println("Hostname: " + InetAddress.getLocalHost().getHostName());
						System.out.println("Port number: " + myPortNo);
					} catch (UnknownHostException e) {
						System.err.println("Could not read host name and port number: " + e.getMessage());
						System.exit(1);
					}
                	
                	//Serve the world
                	multiThreadedParticipantServer.serve();
                }
            };

            new Thread(simple).start();
			
		}catch(Exception ouch) {
			System.err.println("Could not start server: " + ouch.getMessage());
			ouch.printStackTrace();
			System.exit(1);
		}
		
	}

}
