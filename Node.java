package db.core;

import db.consensus.RaftState;
import db.storage.StorageEngine;
import db.network.NetworkHandler;
import db.transaction.TransactionManager;
import db.query.QueryProcessor;

import java.util.*;
import java.util.concurrent.*;
import java.net.InetSocketAddress;
import java.io.IOException;

public class Node {
    private final String nodeId;
    private final InetSocketAddress address;
    private final Set<InetSocketAddress> cluster;
    private final RaftState raftState;
    private final StorageEngine storage;
    private final NetworkHandler network;
    private final TransactionManager txnManager;
    private final QueryProcessor queryProcessor;
    private final ScheduledExecutorService scheduler;
    
    private volatile boolean running;
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> log;
    private int commitIndex;
    private int lastApplied;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;
    
    public Node(String nodeId, InetSocketAddress address, Set<InetSocketAddress> cluster) {
        this.nodeId = nodeId;
        this.address = address;
        this.cluster = new HashSet<>(cluster);
        this.raftState = new RaftState();
        this.storage = new StorageEngine();
        this.network = new NetworkHandler(this);
        this.txnManager = new TransactionManager(this);
        this.queryProcessor = new QueryProcessor(this);
        this.scheduler = Executors.newScheduledThreadPool(4);
        
        this.running = false;
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        
        initializeClusterState();
    }
    
    private void initializeClusterState() {
        for (InetSocketAddress peer : cluster) {
            String peerId = peer.toString();
            nextIndex.put(peerId, 0);
            matchIndex.put(peerId, -1);
        }
    }
    
    public void start() throws IOException {
        if (running) return;
        
        running = true;
        network.start();
        storage.initialize();
        
        scheduler.scheduleAtFixedRate(this::heartbeatTick, 50, 50, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::electionTick, 150, 150, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::applyCommittedEntries, 10, 10, TimeUnit.MILLISECONDS);
    }
    
    public void stop() {
        running = false;
        scheduler.shutdown();
        network.stop();
        storage.close();
    }
    
    private void heartbeatTick() {
        if (raftState.isLeader()) {
            sendHeartbeats();
        }
    }
    
    private void electionTick() {
        if (raftState.isFollower() && raftState.hasElectionTimeout()) {
            startElection();
        }
    }
    
    private void startElection() {
        currentTerm++;
        raftState.becomeCandidate();
        votedFor = nodeId;
        raftState.resetElectionTimeout();
        
        int votes = 1;
        CompletableFuture<Integer> voteCount = CompletableFuture.completedFuture(votes);
        
        for (InetSocketAddress peer : cluster) {
            if (!peer.equals(address)) {
                requestVote(peer);
            }
        }
    }
    
    private void requestVote(InetSocketAddress peer) {
        VoteRequest request = new VoteRequest(
            currentTerm, 
            nodeId, 
            log.size() - 1, 
            log.isEmpty() ? 0 : log.get(log.size() - 1).term
        );
        
        network.sendVoteRequest(peer, request);
    }
    
    public VoteResponse handleVoteRequest(VoteRequest request) {
        if (request.term > currentTerm) {
            currentTerm = request.term;
            votedFor = null;
            raftState.becomeFollower();
        }
        
        boolean voteGranted = false;
        if (request.term == currentTerm && 
            (votedFor == null || votedFor.equals(request.candidateId)) &&
            isLogUpToDate(request.lastLogIndex, request.lastLogTerm)) {
            
            votedFor = request.candidateId;
            voteGranted = true;
            raftState.resetElectionTimeout();
        }
        
        return new VoteResponse(currentTerm, voteGranted);
    }
    
    private boolean isLogUpToDate(int lastLogIndex, long lastLogTerm) {
        if (log.isEmpty()) return true;
        
        LogEntry lastEntry = log.get(log.size() - 1);
        return lastLogTerm > lastEntry.term || 
               (lastLogTerm == lastEntry.term && lastLogIndex >= log.size() - 1);
    }
    
    public void handleVoteResponse(VoteResponse response, InetSocketAddress from) {
        if (!raftState.isCandidate() || response.term > currentTerm) {
            if (response.term > currentTerm) {
                currentTerm = response.term;
                raftState.becomeFollower();
                votedFor = null;
            }
            return;
        }
        
        if (response.voteGranted) {
            int votes = raftState.incrementVoteCount();
            if (votes > cluster.size() / 2) {
                becomeLeader();
            }
        }
    }
    
    private void becomeLeader() {
        raftState.becomeLeader();
        
        for (String peerId : nextIndex.keySet()) {
            nextIndex.put(peerId, log.size());
            matchIndex.put(peerId, -1);
        }
        
        sendHeartbeats();
    }
    
    private void sendHeartbeats() {
        for (InetSocketAddress peer : cluster) {
            if (!peer.equals(address)) {
                sendAppendEntries(peer, Collections.emptyList());
            }
        }
    }
    
    private void sendAppendEntries(InetSocketAddress peer, List<LogEntry> entries) {
        String peerId = peer.toString();
        int prevLogIndex = nextIndex.get(peerId) - 1;
        long prevLogTerm = prevLogIndex >= 0 && prevLogIndex < log.size() ? 
                          log.get(prevLogIndex).term : 0;
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            currentTerm,
            nodeId,
            prevLogIndex,
            prevLogTerm,
            entries,
            commitIndex
        );
        
        network.sendAppendEntries(peer, request);
    }
    
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.term >= currentTerm) {
            currentTerm = request.term;
            raftState.becomeFollower();
            raftState.resetElectionTimeout();
        }
        
        if (request.term < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }
        
        if (request.prevLogIndex >= 0 && 
            (request.prevLogIndex >= log.size() || 
             log.get(request.prevLogIndex).term != request.prevLogTerm)) {
            return new AppendEntriesResponse(currentTerm, false);
        }
        
        int logIndex = request.prevLogIndex + 1;
        for (LogEntry entry : request.entries) {
            if (logIndex < log.size() && log.get(logIndex).term != entry.term) {
                log.subList(logIndex, log.size()).clear();
            }
            if (logIndex >= log.size()) {
                log.add(entry);
            }
            logIndex++;
        }
        
        if (request.leaderCommit > commitIndex) {
            commitIndex = Math.min(request.leaderCommit, log.size() - 1);
        }
        
        return new AppendEntriesResponse(currentTerm, true);
    }
    
    public void handleAppendEntriesResponse(AppendEntriesResponse response, InetSocketAddress from) {
        String peerId = from.toString();
        
        if (response.term > currentTerm) {
            currentTerm = response.term;
            raftState.becomeFollower();
            votedFor = null;
            return;
        }
        
        if (!raftState.isLeader() || response.term < currentTerm) {
            return;
        }
        
        if (response.success) {
            nextIndex.put(peerId, nextIndex.get(peerId) + 1);
            matchIndex.put(peerId, nextIndex.get(peerId) - 1);
            updateCommitIndex();
        } else {
            nextIndex.put(peerId, Math.max(0, nextIndex.get(peerId) - 1));
        }
    }
    
    private void updateCommitIndex() {
        for (int n = commitIndex + 1; n < log.size(); n++) {
            if (log.get(n).term != currentTerm) continue;
            
            int count = 1;
            for (int index : matchIndex.values()) {
                if (index >= n) count++;
            }
            
            if (count > cluster.size() / 2) {
                commitIndex = n;
            }
        }
    }
    
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            applyEntry(entry);
        }
    }
    
    private void applyEntry(LogEntry entry) {
        switch (entry.type) {
            case WRITE:
                storage.put(entry.key, entry.value);
                break;
            case DELETE:
                storage.delete(entry.key);
                break;
            case TRANSACTION_BEGIN:
                txnManager.beginTransaction(entry.transactionId);
                break;
            case TRANSACTION_COMMIT:
                txnManager.commitTransaction(entry.transactionId);
                break;
            case TRANSACTION_ABORT:
                txnManager.abortTransaction(entry.transactionId);
                break;
        }
    }
    
    public CompletableFuture<String> executeQuery(String query) {
        return queryProcessor.process(query);
    }
    
    public CompletableFuture<Void> replicate(LogEntry entry) {
        if (!raftState.isLeader()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Not leader"));
            return future;
        }
        
        log.add(entry);
        
        CompletableFuture<Void> replicationFuture = new CompletableFuture<>();
        
        for (InetSocketAddress peer : cluster) {
            if (!peer.equals(address)) {
                sendAppendEntries(peer, Collections.singletonList(entry));
            }
        }
        
        scheduler.schedule(() -> {
            if (commitIndex >= log.size() - 1) {
                replicationFuture.complete(null);
            } else {
                replicationFuture.completeExceptionally(new RuntimeException("Replication failed"));
            }
        }, 1, TimeUnit.SECONDS);
        
        return replicationFuture;
    }
    
    public StorageEngine getStorage() { return storage; }
    public TransactionManager getTransactionManager() { return txnManager; }
    public boolean isLeader() { return raftState.isLeader(); }
    public String getNodeId() { return nodeId; }
    public long getCurrentTerm() { return currentTerm; }
}

class VoteRequest {
    final long term;
    final String candidateId;
    final int lastLogIndex;
    final long lastLogTerm;
    
    VoteRequest(long term, String candidateId, int lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}

class VoteResponse {
    final long term;
    final boolean voteGranted;
    
    VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}

class AppendEntriesRequest {
    final long term;
    final String leaderId;
    final int prevLogIndex;
    final long prevLogTerm;
    final List<LogEntry> entries;
    final int leaderCommit;
    
    AppendEntriesRequest(long term, String leaderId, int prevLogIndex, long prevLogTerm, 
                        List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
}

class AppendEntriesResponse {
    final long term;
    final boolean success;
    
    AppendEntriesResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }
}

enum LogEntryType {
    WRITE, DELETE, TRANSACTION_BEGIN, TRANSACTION_COMMIT, TRANSACTION_ABORT
}

class LogEntry {
    final LogEntryType type;
    final long term;
    final String key;
    final byte[] value;
    final String transactionId;
    final long timestamp;
    
    LogEntry(LogEntryType type, long term, String key, byte[] value, String transactionId) {
        this.type = type;
        this.term = term;
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.timestamp = System.currentTimeMillis();
    }
}
