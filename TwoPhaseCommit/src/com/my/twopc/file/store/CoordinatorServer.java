package com.my.twopc.file.store;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.my.twopc.file.store.FileStore.Iface;
import com.my.twopc.file.store.impl.CoordinatorImpl;
import com.my.twopc.model.ReplicaInfo;

public class CoordinatorServer {

    private static final int NUM_REPLICAS = 3;

    public static void main(String[] args) {
        // Get hostname and port number from user
        List<ReplicaInfo> participantList = getReplicaInfoFromUser();

        try {
        	CoordinatorImpl handler = new CoordinatorImpl(participantList);
            FileStore.Processor<Iface> processor = new FileStore.Processor<Iface>(handler);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
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

    private static void simple(final FileStore.Processor<Iface> processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(0);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Server address: " + InetAddress.getLocalHost().getHostName());
            server.serve();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

