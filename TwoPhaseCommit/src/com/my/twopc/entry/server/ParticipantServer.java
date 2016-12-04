package com.my.twopc.entry.server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.my.twopc.participant.store.Participant;
import com.my.twopc.participant.store.Participant.Iface;
import com.my.twopc.participant.store.impl.ParticipantImpl;

public class ParticipantServer {

	public static void main(String[] args) {
		//Check for valid arguments
		if(args.length < 2) {
			System.err.println("Invalid arguments!\nPlease enter Coordinator's hostname and port number as cmdline arguments");
			System.exit(1);
		}
		
		String coordHostname = args[0];
		int coordPortno = Integer.parseInt(args[1]);
		
		startAndServe(coordHostname, coordPortno);
	}

	private static void startAndServe(String coordHostname, int coordPortno) {
		try {
			//Create server socket
			TServerTransport serverTransport = new TServerSocket(0);

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
