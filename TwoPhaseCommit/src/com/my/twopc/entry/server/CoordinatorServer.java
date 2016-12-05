package com.my.twopc.entry.server;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.my.twopc.coordinator.store.Coordinator;
import com.my.twopc.coordinator.store.Coordinator.Iface;
import com.my.twopc.coordinator.store.impl.CoordinatorImpl;
import com.my.twopc.model.ReplicaInfo;

public class CoordinatorServer {

    public static void main(String[] args) {
    	if(args.length < 2) {
    		System.err.println("Invalid arguments!\n<PORT_NO> <REPLICA_FILE>");
    		System.exit(1);
    	}
    	
    	int serverPort = Integer.parseInt(args[0]);

        // Get hostname and port number from user
        List<ReplicaInfo> participantList = getReplicaInfoFromUser(args[1]);

        String testNum = null;
        if(args.length >= 3) {
        	testNum = args[2];
        }
        try {
        	CoordinatorImpl handler = new CoordinatorImpl(participantList, testNum);
            Coordinator.Processor<Iface> processor = new Coordinator.Processor<Iface>(handler);

            Runnable simple = new Runnable() {
                public void run() {
                	System.out.println("Starting Coordinator server");
                    simple(processor, serverPort);
                }
            };

            new Thread(simple).start();

        } catch (Exception x) {
            System.out.println(x.getMessage());
            x.printStackTrace();
        }

    }

    private static List<ReplicaInfo> getReplicaInfoFromUser(String replicaInfoFile) {
    	List<ReplicaInfo> participantList = new ArrayList<>();
    	try {
    		Scanner sc = new Scanner(new File(replicaInfoFile));
    		while(sc.hasNextLine()) {
    			String tokens[] = sc.nextLine().split(" ");
    			participantList.add(new ReplicaInfo(tokens[0], Integer.parseInt(tokens[1])));
    		}

    		sc.close();
    	}catch(Exception ouch) {
    		System.err.println("Could not read participant file: " + ouch.getMessage());
    		ouch.printStackTrace();
    		System.exit(1);
    	}

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

