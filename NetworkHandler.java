package db.network;

import db.core.*;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.*;
import java.util.*;

public class NetworkHandler {
    private final Node node;
    private ServerSocketChannel serverChannel;
    private Selector selector;
    private final ExecutorService threadPool;
    private final Map<InetSocketAddress, SocketChannel> connections;
    private volatile boolean running;
    
    private static final int BUFFER_SIZE = 8192;
    private static final int THREAD_POOL_SIZE = 10;
    
    public NetworkHandler(Node node) {
        this.node = node;
        this.threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.connections = new ConcurrentHashMap<>();
        this.running = false;
    }
    
    public void start() throws IOException {
        if (running) return;
        
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        
        InetSocketAddress address = new InetSocketAddress("localhost", getPort());
        serverChannel.bind(address);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        
        running = true;
        
        threadPool.submit(this::eventLoop);
    }
    
    private int getPort() {
        return 8080 + Math.abs(node.getNodeId().hashCode() % 1000);
    }
    
    private void eventLoop() {
        while (running) {
            try {
                selector.select(1000);
                
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();
                    
                    if (!key.isValid()) continue;
                    
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("Network error: " + e.getMessage());
                }
            }
        }
    }
    
    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ, new ConnectionState());
        }
    }
    
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionState state = (ConnectionState) key.attachment();
        
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead = channel.read(buffer);
        
        if (bytesRead == -1) {
            channel.close();
            key.cancel();
            return;
        }
        
        if (bytesRead > 0) {
            buffer.flip();
            state.inputBuffer.put(buffer);
            
            processMessages(channel, state);
        }
    }
    
    private void processMessages(SocketChannel channel, ConnectionState state) {
        state.inputBuffer.flip();
        
        while (state.inputBuffer.remaining() >= 8) {
            int messageType = state.inputBuffer.getInt();
            int messageLength = state.inputBuffer.getInt();
            
            if (state.inputBuffer.remaining() < messageLength) {
                state.inputBuffer.position(state.inputBuffer.position() - 8);
                break;
            }
            
            byte[] messageData = new byte[messageLength];
            state.inputBuffer.get(messageData);
            
            threadPool.submit(() -> processMessage(channel, messageType, messageData));
        }
        
        state.inputBuffer.compact();
    }
    
    private void processMessage(SocketChannel channel, int messageType, byte[] data) {
        try {
            switch (messageType) {
                case MessageType.VOTE_REQUEST:
                    handleVoteRequest(channel, data);
                    break;
                case MessageType.VOTE_RESPONSE:
                    handleVoteResponse(channel, data);
                    break;
                case MessageType.APPEND_ENTRIES_REQUEST:
                    handleAppendEntriesRequest(channel, data);
                    break;
                case MessageType.APPEND_ENTRIES_RESPONSE:
                    handleAppendEntriesResponse(channel, data);
                    break;
                case MessageType.CLIENT_REQUEST:
                    handleClientRequest(channel, data);
                    break;
                case MessageType.CLIENT_RESPONSE:
                    handleClientResponse(channel, data);
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }
    
    private void handleVoteRequest(SocketChannel channel, byte[] data) throws IOException {
        VoteRequest request = deserializeVoteRequest(data);
        VoteResponse response = node.handleVoteRequest(request);
        
        byte[] responseData = serializeVoteResponse(response);
        sendMessage(channel, MessageType.VOTE_RESPONSE, responseData);
    }
    
    private void handleVoteResponse(SocketChannel channel, byte[] data) {
        VoteResponse response = deserializeVoteResponse(data);
        InetSocketAddress from = getRemoteAddress(channel);
        node.handleVoteResponse(response, from);
    }
    
    private void handleAppendEntriesRequest(SocketChannel channel, byte[] data) throws IOException {
        AppendEntriesRequest request = deserializeAppendEntriesRequest(data);
        AppendEntriesResponse response = node.handleAppendEntries(request);
        
        byte[] responseData = serializeAppendEntriesResponse(response);
        sendMessage(channel, MessageType.APPEND_ENTRIES_RESPONSE, responseData);
    }
    
    private void handleAppendEntriesResponse(SocketChannel channel, byte[] data) {
        AppendEntriesResponse response = deserializeAppendEntriesResponse(data);
        InetSocketAddress from = getRemoteAddress(channel);
        node.handleAppendEntriesResponse(response, from);
    }
    
    private void handleClientRequest(SocketChannel channel, byte[] data) throws IOException {
        ClientRequest request = deserializeClientRequest(data);
        
        node.executeQuery(request.query)
            .thenAccept(result -> {
                try {
                    ClientResponse response = new ClientResponse(result, null);
                    byte[] responseData = serializeClientResponse(response);
                    sendMessage(channel, MessageType.CLIENT_RESPONSE, responseData);
                } catch (IOException e) {
                    System.err.println("Error sending client response: " + e.getMessage());
                }
            })
            .exceptionally(ex -> {
                try {
                    ClientResponse response = new ClientResponse(null, ex.getMessage());
                    byte[] responseData = serializeClientResponse(response);
                    sendMessage(channel, MessageType.CLIENT_RESPONSE, responseData);
                } catch (IOException e) {
                    System.err.println("Error sending error response: " + e.getMessage());
                }
                return null;
            });
    }
    
    private void handleClientResponse(SocketChannel channel, byte[] data) {
        // Handle client response if needed
    }
    
    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionState state = (ConnectionState) key.attachment();
        
        synchronized (state.outputBuffer) {
            state.outputBuffer.flip();
            channel.write(state.outputBuffer);
            
            if (state.outputBuffer.hasRemaining()) {
                state.outputBuffer.compact();
            } else {
                state.outputBuffer.clear();
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }
    
    public void sendVoteRequest(InetSocketAddress target, VoteRequest request) {
        try {
            SocketChannel channel = getConnection(target);
            byte[] data = serializeVoteRequest(request);
            sendMessage(channel, MessageType.VOTE_REQUEST, data);
        } catch (IOException e) {
            System.err.println("Failed to send vote request: " + e.getMessage());
        }
    }
    
    public void sendAppendEntries(InetSocketAddress target, AppendEntriesRequest request) {
        try {
            SocketChannel channel = getConnection(target);
            byte[] data = serializeAppendEntriesRequest(request);
            sendMessage(channel, MessageType.APPEND_ENTRIES_REQUEST, data);
        } catch (IOException e) {
            System.err.println("Failed to send append entries: " + e.getMessage());
        }
    }
    
    private SocketChannel getConnection(InetSocketAddress target) throws IOException {
        SocketChannel channel = connections.get(target);
        if (channel == null || !channel.isConnected()) {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(target);
            
            while (!channel.finishConnect()) {
                Thread.yield();
            }
            
            connections.put(target, channel);
        }
        return channel;
    }
    
    private void sendMessage(SocketChannel channel, int messageType, byte[] data) throws IOException {
        ByteBuffer message = ByteBuffer.allocate(8 + data.length);
        message.putInt(messageType);
        message.putInt(data.length);
        message.put(data);
        message.flip();
        
        while (message.hasRemaining()) {
            channel.write(message);
        }
    }
    
    private InetSocketAddress getRemoteAddress(SocketChannel channel) {
        try {
            return (InetSocketAddress) channel.getRemoteAddress();
        } catch (IOException e) {
            return null;
        }
    }
    
    private byte[] serializeVoteRequest(VoteRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(request.term);
        dos.writeUTF(request.candidateId);
        dos.writeInt(request.lastLogIndex);
        dos.writeLong(request.lastLogTerm);
        
        return baos.toByteArray();
    }
    
    private VoteRequest deserializeVoteRequest(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            long term = dis.readLong();
            String candidateId = dis.readUTF();
            int lastLogIndex = dis.readInt();
            long lastLogTerm = dis.readLong();
            
            return new VoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize vote request", e);
        }
    }
    
    private byte[] serializeVoteResponse(VoteResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(response.term);
        dos.writeBoolean(response.voteGranted);
        
        return baos.toByteArray();
    }
    
    private VoteResponse deserializeVoteResponse(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            long term = dis.readLong();
            boolean voteGranted = dis.readBoolean();
            
            return new VoteResponse(term, voteGranted);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize vote response", e);
        }
    }
    
    private byte[] serializeAppendEntriesRequest(AppendEntriesRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(request.term);
        dos.writeUTF(request.leaderId);
        dos.writeInt(request.prevLogIndex);
        dos.writeLong(request.prevLogTerm);
        dos.writeInt(request.leaderCommit);
        
        dos.writeInt(request.entries.size());
        for (LogEntry entry : request.entries) {
            serializeLogEntry(dos, entry);
        }
        
        return baos.toByteArray();
    }
    
    private AppendEntriesRequest deserializeAppendEntriesRequest(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            long term = dis.readLong();
            String leaderId = dis.readUTF();
            int prevLogIndex = dis.readInt();
            long prevLogTerm = dis.readLong();
            int leaderCommit = dis.readInt();
            
            int entriesCount = dis.readInt();
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 0; i < entriesCount; i++) {
                entries.add(deserializeLogEntry(dis));
            }
            
            return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize append entries request", e);
        }
    }
    
    private byte[] serializeAppendEntriesResponse(AppendEntriesResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeLong(response.term);
        dos.writeBoolean(response.success);
        
        return baos.toByteArray();
    }
    
    private AppendEntriesResponse deserializeAppendEntriesResponse(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            long term = dis.readLong();
            boolean success = dis.readBoolean();
            
            return new AppendEntriesResponse(term, success);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize append entries response", e);
        }
    }
    
    private void serializeLogEntry(DataOutputStream dos, LogEntry entry) throws IOException {
        dos.writeInt(entry.type.ordinal());
        dos.writeLong(entry.term);
        dos.writeLong(entry.timestamp);
        
        if (entry.key != null) {
            dos.writeBoolean(true);
            dos.writeUTF(entry.key);
        } else {
            dos.writeBoolean(false);
        }
        
        if (entry.value != null) {
            dos.writeBoolean(true);
            dos.writeInt(entry.value.length);
            dos.write(entry.value);
        } else {
            dos.writeBoolean(false);
        }
        
        if (entry.transactionId != null) {
            dos.writeBoolean(true);
            dos.writeUTF(entry.transactionId);
        } else {
            dos.writeBoolean(false);
        }
    }
    
    private LogEntry deserializeLogEntry(DataInputStream dis) throws IOException {
        LogEntryType type = LogEntryType.values()[dis.readInt()];
        long term = dis.readLong();
        long timestamp = dis.readLong();
        
        String key = null;
        if (dis.readBoolean()) {
            key = dis.readUTF();
        }
        
        byte[] value = null;
        if (dis.readBoolean()) {
            int valueLength = dis.readInt();
            value = new byte[valueLength];
            dis.readFully(value);
        }
        
        String transactionId = null;
        if (dis.readBoolean()) {
            transactionId = dis.readUTF();
        }
        
        return new LogEntry(type, term, key, value, transactionId);
    }
    
    private byte[] serializeClientRequest(ClientRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeUTF(request.query);
        
        return baos.toByteArray();
    }
    
    private ClientRequest deserializeClientRequest(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            String query = dis.readUTF();
            
            return new ClientRequest(query);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize client request", e);
        }
    }
    
    private byte[] serializeClientResponse(ClientResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        if (response.result != null) {
            dos.writeBoolean(true);
            dos.writeUTF(response.result);
        } else {
            dos.writeBoolean(false);
        }
        
        if (response.error != null) {
            dos.writeBoolean(true);
            dos.writeUTF(response.error);
        } else {
            dos.writeBoolean(false);
        }
        
        return baos.toByteArray();
    }
    
    private ClientResponse deserializeClientResponse(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            String result = null;
            if (dis.readBoolean()) {
                result = dis.readUTF();
            }
            
            String error = null;
            if (dis.readBoolean()) {
                error = dis.readUTF();
            }
            
            return new ClientResponse(result, error);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize client response", e);
        }
    }
    
    public void stop() {
        running = false;
        
        try {
            if (selector != null) {
                selector.close();
            }
            if (serverChannel != null) {
                serverChannel.close();
            }
            
            for (SocketChannel channel : connections.values()) {
                channel.close();
            }
            connections.clear();
            
        } catch (IOException e) {
            System.err.println("Error stopping network handler: " + e.getMessage());
        }
        
        threadPool.shutdown();
    }
    
    private static class ConnectionState {
        final ByteBuffer inputBuffer;
        final ByteBuffer outputBuffer;
        
        ConnectionState() {
            this.inputBuffer = ByteBuffer.allocate(BUFFER_SIZE * 2);
            this.outputBuffer = ByteBuffer.allocate(BUFFER_SIZE * 2);
        }
    }
    
    private static class MessageType {
        static final int VOTE_REQUEST = 1;
        static final int VOTE_RESPONSE = 2;
        static final int APPEND_ENTRIES_REQUEST = 3;
        static final int APPEND_ENTRIES_RESPONSE = 4;
        static final int CLIENT_REQUEST = 5;
        static final int CLIENT_RESPONSE = 6;
    }
    
    static class ClientRequest {
        final String query;
        
        ClientRequest(String query) {
            this.query = query;
        }
    }
    
    static class ClientResponse {
        final String result;
        final String error;
        
        ClientResponse(String result, String error) {
            this.result = result;
            this.error = error;
        }
    }
}
