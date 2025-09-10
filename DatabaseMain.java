package db;

import db.core.Node;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class DatabaseMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java DatabaseMain <nodeId> <port> [peer1:port1] [peer2:port2] ...");
            System.exit(1);
        }
        
        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        
        Set<InetSocketAddress> cluster = new HashSet<>();
        cluster.add(new InetSocketAddress("localhost", port));
        
        for (int i = 2; i < args.length; i++) {
            String[] parts = args[i].split(":");
            String host = parts[0];
            int peerPort = Integer.parseInt(parts[1]);
            cluster.add(new InetSocketAddress(host, peerPort));
        }
        
        Node node = new Node(nodeId, new InetSocketAddress("localhost", port), cluster);
        
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        
        node.start();
        
        System.out.println("Database node " + nodeId + " started on port " + port);
        System.out.println("Cluster: " + cluster);
        System.out.println("Type 'help' for commands or SQL queries:");
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(nodeId + "> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) continue;
            
            if ("quit".equalsIgnoreCase(input) || "exit".equalsIgnoreCase(input)) {
                break;
            }
            
            if ("help".equalsIgnoreCase(input)) {
                printHelp();
                continue;
            }
            
            if ("status".equalsIgnoreCase(input)) {
                printStatus(node);
                continue;
            }
            
            try {
                node.executeQuery(input)
                    .thenAccept(result -> System.out.println("Result: " + result))
                    .exceptionally(ex -> {
                        System.err.println("Error: " + ex.getMessage());
                        return null;
                    })
                    .get();
            } catch (Exception e) {
                System.err.println("Error executing query: " + e.getMessage());
            }
        }
        
        scanner.close();
        node.stop();
    }
    
    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  help                    - Show this help");
        System.out.println("  status                  - Show node status");
        System.out.println("  quit/exit               - Exit the database");
        System.out.println();
        System.out.println("SQL commands:");
        System.out.println("  BEGIN TRANSACTION       - Start a transaction");
        System.out.println("  COMMIT                  - Commit current transaction");
        System.out.println("  ROLLBACK                - Rollback current transaction");
        System.out.println("  SELECT * FROM table     - Query data");
        System.out.println("  INSERT INTO table (cols) VALUES (vals) - Insert data");
        System.out.println("  UPDATE table SET col=val WHERE condition - Update data");
        System.out.println("  DELETE FROM table WHERE condition - Delete data");
    }
    
    private static void printStatus(Node node) {
        System.out.println("Node ID: " + node.getNodeId());
        System.out.println("Leader: " + node.isLeader());
        System.out.println("Term: " + node.getCurrentTerm());
    }
}
