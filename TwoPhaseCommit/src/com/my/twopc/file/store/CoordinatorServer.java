package com.my.twopc.file.store;

import com.my.twopc.file.store.FileStore.Iface;
import com.my.twopc.file.store.impl.CoordinatorImpl;
import com.my.twopc.model.ReplicaInfo;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CoordinatorServer {

    private static CoordinatorImpl handler;

    private static FileStore.Processor<Iface> processor;

    private static int port;

    private static List<ReplicaInfo> participantList = new ArrayList<>();

    private static final int NUM_REPLICAS = 3;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please enter port number.");
            System.exit(1);
        }

        Scanner sc = new Scanner(System.in);

        // Get hostname and port number from user
        getReplicaInfoFromUser(sc);
        sc.close();

        try {
            handler = new CoordinatorImpl(participantList);
            processor = new FileStore.Processor<Iface>(handler);
            port= Integer.valueOf(args[0]);

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

    private static void getReplicaInfoFromUser(Scanner sc) {
        ReplicaInfo replica;
        String hostname;
        int portNumber;

        System.out.println("Please enter details for the 3 participants.");

        for (int i = 0; i < NUM_REPLICAS; i++) {
            System.out.println("Enter hostname and port number for participant " + i + 1);
            hostname = sc.next();
            sc.nextLine();
            portNumber = sc.nextInt();

            replica = new ReplicaInfo(hostname, portNumber);
            participantList.add(replica);
        }
    }

    private static void simple(FileStore.Processor<Iface> processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(port);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Server address: " + InetAddress.getLocalHost().getHostName());
            System.out.println("Port: " + port);
            server.serve();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

