package com.my.twopc.entry.server;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.my.twopc.coordinator.store.Coordinator;
import com.my.twopc.coordinator.store.Coordinator.Iface;
import com.my.twopc.coordinator.store.impl.CoordinatorImpl;
import com.my.twopc.model.ReplicaInfo;

public class CoordinatorServer {

    private static final int NUM_REPLICAS = 3;

    public static void main(String[] args) {
    	if(args.length == 0) {
    		System.err.println("Invalid arguments!\nEnter port number");
    		System.exit(1);
    	}
    	
    	int serverPort = Integer.parseInt(args[0]);

        // Get hostname and port number from user
        List<ReplicaInfo> participantList = getReplicaInfoFromUser();

        try {
        	CoordinatorImpl handler = new CoordinatorImpl(participantList);
            Coordinator.Processor<Iface> processor = new Coordinator.Processor<Iface>(handler);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor, serverPort);
                }
            };

            new Thread(simple).start();

        } catch (Exception x) {
            System.out.println(x.getMessage());
            x.printStackTrace();
        }

    }

    private static List<ReplicaInfo> getReplicaInfoFromUser() {
    	Scanner sc = new Scanner(System.in);
        ReplicaInfo replica;
        String hostname;
        int portNumber;
        List<ReplicaInfo> participantList = new ArrayList<>();

        System.out.println("Please enter details for the 3 participants.");
        for (int i = 0; i < NUM_REPLICAS; i++) {
        	System.out.println("Enter hostname and port number for participant " + (i + 1));
        	System.out.print("Host name: ");
            hostname = sc.nextLine();
            
            System.out.print("Port no.: ");
            portNumber = Integer.parseInt(sc.nextLine());

            replica = new ReplicaInfo(hostname, portNumber);
            participantList.add(replica);

        }
        
        sc.close();

        return participantList;
    }

    private static void simple(final Coordinator.Processor<Iface> processor, int portNo) {
        try {
            TServerSocket serverTransport = new TServerSocket(portNo);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Coordinator Address");
            System.out.println("Hostname: " + InetAddress.getLocalHost().getHostName());
            System.out.println("Port no.: " + portNo);

            server.serve();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

